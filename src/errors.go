package src

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidUserAgent = fmt.Errorf("user agent cannot be empty")
	ErrAuthentication   = errors.New("authentication error")
)

type AuthenticationError struct {
	Code    string
	Message string
}

func (e *AuthenticationError) Error() string {
	if e.Code == "" {
		return fmt.Sprintf("authentication error: %s", e.Message)
	}
	return fmt.Sprintf("authentication error (%s): %s", e.Code, e.Message)
}

func (e *AuthenticationError) Unwrap() error {
	return ErrAuthentication
}

func IsAuthenticationError(err error) bool {
	return errors.Is(err, ErrAuthentication)
}

func NewConnectionError(message string) error {
	return fmt.Errorf("connection error: %s", message)
}

func NewInvalidResponseError(message string) error {
	return fmt.Errorf("invalid response: %s", message)
}

func NewInvalidTokenError(message string) error {
	return &AuthenticationError{Message: message}
}

func NewAuthenticationError(code, message string) error {
	return &AuthenticationError{Code: code, Message: message}
}

func NewTimeoutError(message string) error {
	return fmt.Errorf("timeout error: %s", message)
}
