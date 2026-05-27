package op

import (
	"context"
	"crypto/rand"
    "encoding/hex"
    "encoding/json"
	"errors"
	"sync"
    "strings"
	"time"
	transformerModel "github.com/lingyuins/octopus/internal/transformer/model"
	"github.com/lingyuins/octopus/internal/transformer/outbound"

	"github.com/lingyuins/octopus/internal/db"
	"github.com/lingyuins/octopus/internal/model"
	"github.com/lingyuins/octopus/internal/utils/log"
	"github.com/lingyuins/octopus/internal/utils/snowflake"
	"gorm.io/gorm"
)

const relayLogMaxSize = 20
const relayLogMaxSizeNoDB = 100 // 当不保存到数据库时，允许更大的缓存用于实时查询
const relayLogStreamTokenTTL = 5 * time.Minute

var relayLogCache = make([]model.RelayLog, 0, relayLogMaxSize)
var relayLogCacheLock sync.Mutex

var relayLogFlushLock sync.Mutex

var relayLogSubscribers = make(map[chan model.RelayLog]struct{})
var relayLogSubscribersLock sync.RWMutex

var relayLogStreamTokens = make(map[string]time.Time)
var relayLogStreamTokensLock sync.RWMutex

func RelayLogStreamTokenCreate() (string, error) {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	token := hex.EncodeToString(bytes)
	createdAt := time.Now()

	relayLogStreamTokensLock.Lock()
	relayLogStreamTokens[token] = createdAt
	relayLogStreamTokensLock.Unlock()

	return token, nil
}

func RelayLogStreamTokenVerify(token string) bool {
	now := time.Now()

	relayLogStreamTokensLock.Lock()
	createdAt, ok := relayLogStreamTokens[token]
	if !ok {
		relayLogStreamTokensLock.Unlock()
		return false
	}
	if now.Sub(createdAt) > relayLogStreamTokenTTL {
		delete(relayLogStreamTokens, token)
		relayLogStreamTokensLock.Unlock()
		return false
	}
	relayLogStreamTokensLock.Unlock()
	return true
}

func RelayLogStreamTokenRevoke(token string) {
	relayLogStreamTokensLock.Lock()
	delete(relayLogStreamTokens, token)
	relayLogStreamTokensLock.Unlock()
}

func RelayLogSubscribe() chan model.RelayLog {
	ch := make(chan model.RelayLog, 10)
	relayLogSubscribersLock.Lock()
	relayLogSubscribers[ch] = struct{}{}
	relayLogSubscribersLock.Unlock()
	return ch
}

func RelayLogUnsubscribe(ch chan model.RelayLog) {
	relayLogSubscribersLock.Lock()
	delete(relayLogSubscribers, ch)
	relayLogSubscribersLock.Unlock()
}

func notifySubscribers(relayLog model.RelayLog) {
	relayLogSubscribersLock.RLock()
	defer relayLogSubscribersLock.RUnlock()

	for ch := range relayLogSubscribers {
		select {
		case ch <- relayLog:
		default:
		}
	}
}

func relayLogStreamTokenCleanup(now time.Time) {
	relayLogStreamTokensLock.Lock()
	for token, createdAt := range relayLogStreamTokens {
		if now.Sub(createdAt) > relayLogStreamTokenTTL {
			delete(relayLogStreamTokens, token)
		}
	}
	relayLogStreamTokensLock.Unlock()
}

func relayLogFlushToDB(ctx context.Context) error {
	relayLogFlushLock.Lock()
	defer relayLogFlushLock.Unlock()

	relayLogCacheLock.Lock()
	if len(relayLogCache) == 0 {
		relayLogCacheLock.Unlock()
		return nil
	}
	batch := make([]model.RelayLog, len(relayLogCache))
	copy(batch, relayLogCache)
	// 记录 batch 中最后一条日志的 ID，用于安全截断
	lastFlushedID := batch[len(batch)-1].ID
	relayLogCacheLock.Unlock()

	result := db.GetDB().WithContext(ctx).Create(&batch)
	if result.Error != nil {
		return result.Error
	}

	relayLogCacheLock.Lock()
	// 安全截断：只移除 ID <= lastFlushedID 的前缀部分
	cutIdx := 0
	for i, l := range relayLogCache {
		if l.ID == lastFlushedID {
			cutIdx = i + 1
			break
		}
		if l.ID > lastFlushedID {
			// 遇到比 batch 更新的日志，说明截断点已过
			break
		}
	}
	if cutIdx > 0 {
		relayLogCache = relayLogCache[cutIdx:]
	}
	if len(relayLogCache) == 0 {
		relayLogCache = make([]model.RelayLog, 0, relayLogMaxSize)
	}
	relayLogCacheLock.Unlock()

	return nil
}

func RelayLogAdd(ctx context.Context, relayLog model.RelayLog) error {
	enabled, err := SettingGetBool(model.SettingKeyRelayLogKeepEnabled)
	if err != nil {
		return err
	}
	maxSize := relayLogMaxSize
	if !enabled {
		maxSize = relayLogMaxSizeNoDB
	}
	relayLog.ID = snowflake.GenerateID()
	go notifySubscribers(relayLog)

	relayLogCacheLock.Lock()
	relayLogCache = append(relayLogCache, relayLog)
	if len(relayLogCache) >= maxSize {
		if enabled {
			relayLogCacheLock.Unlock()
			return relayLogFlushToDB(ctx)
		}
		// 如果未启用日志保存，移除最旧的日志，保留最新的日志用于实时查询
		keepSize := maxSize / 2
		if len(relayLogCache) > keepSize {
			relayLogCache = relayLogCache[len(relayLogCache)-keepSize:]
		}
	}
	relayLogCacheLock.Unlock()
	return nil
}

func RelayLogSaveDBTask(ctx context.Context) error {
	log.Debugf("relay log save db task started")
	startTime := time.Now()
	defer func() {
		log.Debugf("relay log save db task finished, save time: %s", time.Since(startTime))
	}()
	now := time.Now()
	defer relayLogStreamTokenCleanup(now)
	enabled, err := SettingGetBool(model.SettingKeyRelayLogKeepEnabled)
	if err != nil {
		return err
	}

	if enabled {
		if err := relayLogFlushToDB(ctx); err != nil {
			return err
		}
		return relayLogCleanup(ctx)
	}

	// 如果未启用日志保存，检查缓存大小，如果超过限制则清理旧日志
	relayLogCacheLock.Lock()
	if len(relayLogCache) > relayLogMaxSizeNoDB {
		keepSize := relayLogMaxSizeNoDB / 2
		relayLogCache = relayLogCache[len(relayLogCache)-keepSize:]
	}
	relayLogCacheLock.Unlock()

	return nil
}

func relayLogCleanup(ctx context.Context) error {
	keepPeriod, err := SettingGetInt(model.SettingKeyRelayLogKeepPeriod)
	if err != nil {
		return err
	}

	if keepPeriod <= 0 {
		return nil
	}

	cutoffTime := time.Now().Add(-time.Duration(keepPeriod) * 24 * time.Hour).Unix()
	return db.GetDB().WithContext(ctx).Where("time < ?", cutoffTime).Delete(&model.RelayLog{}).Error
}

// RelayLogList 查询日志列表，支持可选的时间范围过滤
// startTime 和 endTime 为 nil 时表示不限制时间范围
// 返回轻量条目，不包含 request_content 和 response_content 大字段
func RelayLogList(ctx context.Context, startTime, endTime *int, page, pageSize int) ([]model.RelayLogListItem, error) {
    enabled, err := SettingGetBool(model.SettingKeyRelayLogKeepEnabled)
    if err != nil {
        return nil, err
    }
    hasTimeFilter := startTime != nil || endTime != nil

    matchesTime := func(log model.RelayLog) bool {
        if startTime != nil && log.Time < int64(*startTime) {
            return false
        }
        if endTime != nil && log.Time > int64(*endTime) {
            return false
        }
        return true
    }

    relayLogCacheLock.Lock()
    var cachedLogs []model.RelayLog
    for _, log := range relayLogCache {
        if hasTimeFilter && !matchesTime(log) {
            continue
        }
        cachedLogs = append(cachedLogs, log)
    }
    relayLogCacheLock.Unlock()

    cacheCount := len(cachedLogs)
    offset := (page - 1) * pageSize

    var result []model.RelayLogListItem
    if offset < cacheCount {
        cacheTake := min(pageSize, cacheCount-offset)
        start := cacheCount - offset - 1
        for i := 0; i < cacheTake; i++ {
            idx := start - i
            if idx < 0 {
                break
            }
            item := cachedLogs[idx].ToListItem()
            if channel, getErr := ChannelGet(item.ChannelId, ctx); getErr == nil {
                fillRelayLogListItemRequestTypeLabel(&item, channel.Type, cachedLogs[idx].RequestContent)
            }
            result = append(result, item)
        }
    }

    if enabled {
        remaining := pageSize - len(result)
        if remaining > 0 {
            dbOffset := 0
            if offset > cacheCount {
                dbOffset = offset - cacheCount
            }
            query := db.GetDB().WithContext(ctx).
                Select("id", "time", "request_model_name", "request_api_key_name",
                    "channel_id", "channel_name", "actual_model_name",
                    "input_tokens", "output_tokens", "ftut", "use_time",
                    "cost", "error", "attempts", "total_attempts", "request_content")
            if startTime != nil {
                query = query.Where("time >= ?", *startTime)
            }
            if endTime != nil {
                query = query.Where("time <= ?", *endTime)
            }

            var dbLogs []model.RelayLog
            if err := query.Order("id DESC").Offset(dbOffset).Limit(remaining).Find(&dbLogs).Error; err != nil {
                return nil, err
            }
            for i := range dbLogs {
                item := dbLogs[i].ToListItem()
                if channel, getErr := ChannelGet(item.ChannelId, ctx); getErr == nil {
                    fillRelayLogListItemRequestTypeLabel(&item, channel.Type, dbLogs[i].RequestContent)
                }
                result = append(result, item)
            }
        }
    }

    return result, nil
}
func RelayLogClear(ctx context.Context) error {
	relayLogCacheLock.Lock()
	relayLogCache = make([]model.RelayLog, 0, relayLogMaxSize)
	relayLogCacheLock.Unlock()
	return db.GetDB().WithContext(ctx).Where("1 = 1").Delete(&model.RelayLog{}).Error
}

// RelayLogGetByID 根据ID获取完整日志详情（包含 request_content 和 response_content）
func RelayLogGetByID(ctx context.Context, id int64) (*model.RelayLog, error) {
    var relayLog model.RelayLog
    if err := db.GetDB().WithContext(ctx).Where("id = ?", id).First(&relayLog).Error; err != nil {
        if !errors.Is(err, gorm.ErrRecordNotFound) {
            return nil, err
        }

        relayLogCacheLock.Lock()
        defer relayLogCacheLock.Unlock()
        for i := range relayLogCache {
            if relayLogCache[i].ID == id {
                cached := relayLogCache[i]
                if channel, getErr := ChannelGet(cached.ChannelId, ctx); getErr == nil {
                    fillRelayLogRequestTypeLabel(&cached, channel.Type)
                }
                return &cached, nil
            }
        }
        return nil, nil
    }
    if channel, getErr := ChannelGet(relayLog.ChannelId, ctx); getErr == nil {
        fillRelayLogRequestTypeLabel(&relayLog, channel.Type)
    }
    return &relayLog, nil
}

func detectRelayLogRequestTypeLabel(channelType outbound.OutboundType, requestContent string) string {
    switch channelType {
    case outbound.OutboundTypeMiMoChat:
        return "MiMo Chat"
    case outbound.OutboundTypeOpenAIResponse:
        return "Responses"
    case outbound.OutboundTypeOpenAIEmbedding:
        return "Embedding"
    case outbound.OutboundTypeAnthropic:
        return "Anthropic Messages"
    case outbound.OutboundTypeGemini:
        return "Gemini"
    case outbound.OutboundTypeVolcengine:
        return "Volcengine"
    }

    if strings.TrimSpace(requestContent) == "" {
        if channelType == outbound.OutboundTypeOpenAIChat {
            return "对话"
        }
        return "未知"
    }

    var req transformerModel.InternalLLMRequest
    if err := json.Unmarshal([]byte(requestContent), &req); err == nil {
        if req.Stream != nil && *req.Stream {
            return "流式对话"
        }
        if req.RawAPIFormat == transformerModel.APIFormatOpenAIEmbedding || req.EmbeddingInput != nil {
            return "Embedding"
        }
        if req.RawAPIFormat == transformerModel.APIFormatOpenAIResponse {
            return "Responses"
        }
    }

    if channelType == outbound.OutboundTypeOpenAIChat {
        return "对话"
    }
    return "未知"
}

func fillRelayLogRequestTypeLabel(logItem *model.RelayLog, channelType outbound.OutboundType) {
    if logItem == nil {
        return
    }
    logItem.RequestTypeLabel = detectRelayLogRequestTypeLabel(channelType, logItem.RequestContent)
}

func fillRelayLogListItemRequestTypeLabel(logItem *model.RelayLogListItem, channelType outbound.OutboundType, requestContent string) {
    if logItem == nil {
        return
    }
    logItem.RequestTypeLabel = detectRelayLogRequestTypeLabel(channelType, requestContent)
}
