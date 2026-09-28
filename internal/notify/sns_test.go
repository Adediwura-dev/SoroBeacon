package notify

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockSNSClient implements SNSAPI for testing.
type mockSNSClient struct {
	publishFunc func(ctx context.Context, params *sns.PublishInput, optFns ...func(*sns.Options)) (*sns.PublishOutput, error)
}

func (m *mockSNSClient) Publish(ctx context.Context, params *sns.PublishInput, optFns ...func(*sns.Options)) (*sns.PublishOutput, error) {
	if m.publishFunc != nil {
		return m.publishFunc(ctx, params, optFns...)
	}
	return &sns.PublishOutput{MessageId: aws.String("test-message-id")}, nil
}

func TestNewSNSValidConfig(t *testing.T) {
	mockClient := &mockSNSClient{}
	tests := []struct {
		name   string
		config string
	}{
		{"with static credentials", `{"topic_arn": "arn:aws:sns:us-east-1:123456789012:sorobeacon", "region": "us-east-1", "access_key_id": "AKIA...", "secret_access_key": "secret"}`},
		{"without static credentials (uses default chain)", `{"topic_arn": "arn:aws:sns:us-east-1:123456789012:sorobeacon", "region": "us-east-1"}`},
		{"different region", `{"topic_arn": "arn:aws:sns:eu-west-1:123456789012:sorobeacon", "region": "eu-west-1"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, err := NewSNSWithClient(json.RawMessage(tt.config), mockClient)
			require.NoError(t, err)
			require.NotNil(t, n)
			_, ok := n.(*SNS)
			assert.True(t, ok, "NewSNS must return a *SNS")
		})
	}
}

func TestNewSNSMalformedConfig(t *testing.T) {
	_, err := NewSNSWithClient(json.RawMessage(`{"topic_arn": }`), &mockSNSClient{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid config")
}

func TestNewSNSMissingTopicARN(t *testing.T) {
	tests := []struct {
		name   string
		config string
	}{
		{"empty object", `{}`},
		{"empty topic_arn", `{"topic_arn": "", "region": "us-east-1"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewSNSWithClient(json.RawMessage(tt.config), &mockSNSClient{})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "topic_arn is required")
		})
	}
}

func TestNewSNSMissingRegion(t *testing.T) {
	_, err := NewSNSWithClient(json.RawMessage(`{"topic_arn": "arn:aws:sns:us-east-1:123456789012:sorobeacon"}`), &mockSNSClient{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "region is required")
}

func TestNewSNSInvalidTopicARN(t *testing.T) {
	_, err := NewSNSWithClient(json.RawMessage(`{"topic_arn": "invalid-arn", "region": "us-east-1"}`), &mockSNSClient{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be a valid SNS topic ARN")
}

func TestSNSSendSuccess(t *testing.T) {
	var capturedMessage string
	mockClient := &mockSNSClient{
		publishFunc: func(ctx context.Context, params *sns.PublishInput, optFns ...func(*sns.Options)) (*sns.PublishOutput, error) {
			capturedMessage = aws.ToString(params.Message)
			return &sns.PublishOutput{MessageId: aws.String("test-message-id")}, nil
		},
	}

	n, err := NewSNSWithClient(json.RawMessage(`{"topic_arn": "arn:aws:sns:us-east-1:123456789012:sorobeacon", "region": "us-east-1"}`), mockClient)
	require.NoError(t, err)

	alert := Alert{
		ID:          7,
		MonitorID:   9,
		MonitorName: "my-monitor",
		RuleID:      3,
		RuleType:    "event_emitted",
		ContractID:  "CABC123",
		EventID:     "ev-1",
		EventName:   "transfer",
		Ledger:      3721765,
		TxHash:      "4f2a...",
		Payload:     json.RawMessage(`{"amount": "1000"}`),
		Severity:    "warning",
	}
	require.NoError(t, n.Send(context.Background(), alert))

	require.NotEmpty(t, capturedMessage)
	var msg snsMessage
	require.NoError(t, json.Unmarshal([]byte(capturedMessage), &msg))
	assert.Equal(t, int64(9), msg.MonitorID)
	assert.Equal(t, "my-monitor", msg.MonitorName)
	assert.Equal(t, int64(3), msg.RuleID)
	assert.Equal(t, "event_emitted", msg.RuleType)
	assert.Equal(t, "ev-1", msg.EventID)
	assert.Equal(t, "CABC123", msg.ContractID)
	assert.Equal(t, "transfer", msg.EventName)
	assert.Equal(t, uint32(3721765), msg.Ledger)
	assert.Equal(t, "4f2a...", msg.TxHash)
	assert.Equal(t, "warning", msg.Severity)
	var payloadMap map[string]any
	require.NoError(t, json.Unmarshal(msg.Payload, &payloadMap))
	assert.Equal(t, "1000", payloadMap["amount"])
	assert.NotEmpty(t, msg.CreatedAt)
}

func TestSNSSendFailure(t *testing.T) {
	mockClient := &mockSNSClient{
		publishFunc: func(ctx context.Context, params *sns.PublishInput, optFns ...func(*sns.Options)) (*sns.PublishOutput, error) {
			return nil, &types.InvalidParameterException{Message: aws.String("Invalid topic ARN")}
		},
	}

	n, err := NewSNSWithClient(json.RawMessage(`{"topic_arn": "arn:aws:sns:us-east-1:123456789012:sorobeacon", "region": "us-east-1"}`), mockClient)
	require.NoError(t, err)

	err = n.Send(context.Background(), Alert{ID: 1, MonitorName: "m", RuleID: 2, EventID: "e", ContractID: "c", RuleType: "r"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "sns:")
}

func TestSNSCredentialsNotLogged(t *testing.T) {
	const secretKey = "super-secret-access-key"
	mockClient := &mockSNSClient{
		publishFunc: func(ctx context.Context, params *sns.PublishInput, optFns ...func(*sns.Options)) (*sns.PublishOutput, error) {
			return nil, &types.InternalErrorException{Message: aws.String("internal error")}
		},
	}

	n, err := NewSNSWithClient(json.RawMessage(fmt.Sprintf(`{"topic_arn": "arn:aws:sns:us-east-1:123456789012:sorobeacon", "region": "us-east-1", "access_key_id": "AKIA...", "secret_access_key": %q}`, secretKey)), mockClient)
	require.NoError(t, err)

	err = n.Send(context.Background(), Alert{ID: 1, MonitorName: "m", RuleID: 2, EventID: "e", ContractID: "c", RuleType: "r"})
	require.Error(t, err)
	assert.NotContains(t, err.Error(), secretKey, "errors must never leak the secret_access_key")
}

func TestSNSMessageStructure(t *testing.T) {
	var capturedMessage string
	mockClient := &mockSNSClient{
		publishFunc: func(ctx context.Context, params *sns.PublishInput, optFns ...func(*sns.Options)) (*sns.PublishOutput, error) {
			capturedMessage = aws.ToString(params.Message)
			return &sns.PublishOutput{MessageId: aws.String("test-message-id")}, nil
		},
	}

	n, err := NewSNSWithClient(json.RawMessage(`{"topic_arn": "arn:aws:sns:us-east-1:123456789012:sorobeacon", "region": "us-east-1"}`), mockClient)
	require.NoError(t, err)

	alert := Alert{
		ID:          1,
		MonitorID:   1,
		MonitorName: "test-monitor",
		RuleID:      1,
		RuleType:    "value_threshold",
		ContractID:  "CONTRACT123",
		EventID:     "event-456",
		EventName:   "threshold_breached",
		Ledger:      12345,
		TxHash:      "tx-hash-789",
		Severity:    "critical",
	}
	require.NoError(t, n.Send(context.Background(), alert))

	// Verify the message is valid JSON and contains all expected fields
	var msg map[string]any
	require.NoError(t, json.Unmarshal([]byte(capturedMessage), &msg))

	expectedFields := []string{
		"monitor_id", "monitor_name", "rule_id", "rule_type",
		"event_id", "contract_id", "event_name", "ledger",
		"tx_hash", "payload", "created_at", "severity",
	}
	for _, field := range expectedFields {
		assert.Contains(t, msg, field, "message must contain %s", field)
	}

	// Verify payload is included even when empty (nil serializes to null in JSON)
	assert.Nil(t, msg["payload"])
}