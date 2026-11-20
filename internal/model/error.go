package model

import "fmt"

type Error struct {
	Code   string `json:"code"`
	Detail string `json:"detail"`
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Detail)
}

func Bad(s string) error {
	return &Error{Code: "invalid", Detail: s}
}

func Missing(s string) error {
	return &Error{Code: "missing", Detail: s}
}

func Conflict(s string) error {
	return &Error{Code: "conflict", Detail: s}
}

func Capacity(s string) error {
	return &Error{Code: "capacity", Detail: s}
}
