package mimo

import (
    "bytes"
    "context"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "strings"

    "github.com/lingyuins/octopus/internal/transformer/model"
)

type ChatOutbound struct{}

type mimoMessage struct {
    Role             string                     `json:"role,omitempty"`
    Content          any                        `json:"content,omitempty"`
    Name             *string                    `json:"name,omitempty"`
    Refusal          string                     `json:"refusal,omitempty"`
    ToolCallID       *string                    `json:"tool_call_id,omitempty"`
    ToolCalls        []model.ToolCall           `json:"tool_calls,omitempty"`
    Images           []model.MessageContentPart `json:"images,omitempty"`
    Audio            *struct {
        Data       string `json:"data,omitempty"`
        ExpiresAt  int64  `json:"expires_at,omitempty"`
        ID         string `json:"id,omitempty"`
        Transcript string `json:"transcript,omitempty"`
    } `json:"audio,omitempty"`
    ReasoningContent *string `json:"reasoning_content,omitempty"`
}

type mimoChatRequest struct {
    Messages            []mimoMessage          `json:"messages,omitempty"`
    Model               string                 `json:"model"`
    FrequencyPenalty    *float64               `json:"frequency_penalty,omitempty"`
    Logprobs            *bool                  `json:"logprobs,omitempty"`
    MaxCompletionTokens *int64                 `json:"max_completion_tokens,omitempty"`
    MaxTokens           *int64                 `json:"max_tokens,omitempty"`
    PresencePenalty     *float64               `json:"presence_penalty,omitempty"`
    Seed                *int64                 `json:"seed,omitempty"`
    Store               *bool                  `json:"store,omitzero"`
    Temperature         *float64               `json:"temperature,omitempty"`
    TopLogprobs         *int64                 `json:"top_logprobs,omitzero"`
    TopP                *float64               `json:"top_p,omitempty"`
    ResponseFormat      *model.ResponseFormat  `json:"response_format,omitempty"`
    Stop                *model.Stop            `json:"stop,omitempty"`
    Stream              *bool                  `json:"stream,omitempty"`
    User                *string                `json:"user,omitempty"`
    ReasoningEffort     string                 `json:"reasoning_effort,omitempty"`
    Modalities          []string               `json:"modalities,omitempty"`
    Audio               *struct {
        Format string `json:"format,omitempty"`
        Voice  string `json:"voice,omitempty"`
    } `json:"audio,omitempty"`
    Tools             []model.Tool      `json:"tools,omitempty"`
    ToolChoice        *model.ToolChoice `json:"tool_choice,omitempty"`
    ParallelToolCalls *bool             `json:"parallel_tool_calls,omitempty"`
}

func (o *ChatOutbound) TransformRequest(ctx context.Context, request *model.InternalLLMRequest, baseUrl, key string) (*http.Request, error) {
    request.ClearHelpFields()

    for i := range request.Messages {
        if request.Messages[i].Role == "developer" {
            request.Messages[i].Role = "system"
        }
    }

    payload := mimoChatRequest{
        Model:               request.Model,
        FrequencyPenalty:    request.FrequencyPenalty,
        Logprobs:            request.Logprobs,
        MaxCompletionTokens: request.MaxCompletionTokens,
        MaxTokens:           request.MaxTokens,
        PresencePenalty:     request.PresencePenalty,
        Seed:                request.Seed,
        Store:               request.Store,
        Temperature:         request.Temperature,
        TopLogprobs:         request.TopLogprobs,
        TopP:                request.TopP,
        ResponseFormat:      request.ResponseFormat,
        Stop:                request.Stop,
        Stream:              request.Stream,
        User:                request.User,
        ReasoningEffort:     request.ReasoningEffort,
        Modalities:          request.Modalities,
        Audio:               request.Audio,
        Tools:               request.Tools,
        ToolChoice:          request.ToolChoice,
        ParallelToolCalls:   request.ParallelToolCalls,
        Messages:            make([]mimoMessage, 0, len(request.Messages)),
    }

    for _, msg := range request.Messages {
        payload.Messages = append(payload.Messages, mimoMessage{
            Role:             msg.Role,
            Content:          normalizeMiMoContent(msg.Content),
            Name:             msg.Name,
            Refusal:          msg.Refusal,
            ToolCallID:       msg.ToolCallID,
            ToolCalls:        msg.ToolCalls,
            Images:           msg.Images,
            Audio:            msg.Audio,
            ReasoningContent: msg.ReasoningContent,
        })
    }

    body, err := json.Marshal(payload)
    if err != nil {
        return nil, fmt.Errorf("failed to marshal request: %w", err)
    }

    req, err := http.NewRequestWithContext(ctx, http.MethodPost, "", bytes.NewReader(body))
    if err != nil {
        return nil, fmt.Errorf("failed to create request: %w", err)
    }

    req.Header.Set("Content-Type", "application/json")
    req.Header.Set("Accept", "application/json")
    req.Header.Set("Authorization", "Bearer "+key)
    req.Header.Set("api-key", key)

    parsedURL, err := url.Parse(strings.TrimSuffix(baseUrl, "/"))
    if err != nil {
        return nil, fmt.Errorf("failed to parse base url: %w", err)
    }
    parsedURL.Path = parsedURL.Path + "/chat/completions"
    req.URL = parsedURL
    req.Method = http.MethodPost
    return req, nil
}

func normalizeMiMoContent(content model.MessageContent) any {
    if len(content.MultipleContent) > 0 {
        return content.MultipleContent
    }
    if content.Content != nil {
        return *content.Content
    }
    return nil
}

func (o *ChatOutbound) TransformResponse(ctx context.Context, response *http.Response) (*model.InternalLLMResponse, error) {
    body, err := io.ReadAll(response.Body)
    if err != nil {
        return nil, fmt.Errorf("failed to read response body: %w", err)
    }
    if len(body) == 0 {
        return nil, fmt.Errorf("response body is empty")
    }

    var resp model.InternalLLMResponse
    if err := json.Unmarshal(body, &resp); err != nil {
        return nil, fmt.Errorf("failed to unmarshal response: %w", err)
    }
    return &resp, nil
}

func (o *ChatOutbound) TransformStream(ctx context.Context, eventData []byte) (*model.InternalLLMResponse, error) {
    if bytes.HasPrefix(eventData, []byte("[DONE]")) {
        return &model.InternalLLMResponse{Object: "[DONE]"}, nil
    }

    var errCheck struct {
        Error *model.ErrorDetail `json:"error"`
    }
    if err := json.Unmarshal(eventData, &errCheck); err == nil && errCheck.Error != nil {
        return nil, &model.ResponseError{Detail: *errCheck.Error}
    }

    var resp model.InternalLLMResponse
    if err := json.Unmarshal(eventData, &resp); err != nil {
        return nil, fmt.Errorf("failed to unmarshal stream chunk: %w", err)
    }
    return &resp, nil
}
