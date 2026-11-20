package wire

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"

	"ridgeline/internal/model"
)

func respond(w http.ResponseWriter, value any, err error) {
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		status := http.StatusInternalServerError
		var domain *model.Error
		if errors.As(err, &domain) {
			switch domain.Code {
			case "invalid":
				status = http.StatusBadRequest
			case "missing":
				status = http.StatusNotFound
			case "conflict":
				status = http.StatusConflict
			case "capacity":
				status = http.StatusUnprocessableEntity
			}
		} else {
			log.Printf("request error: %v", err)
			domain = &model.Error{Code: "internal", Detail: "database operation failed"}
		}
		w.WriteHeader(status)
		json.NewEncoder(w).Encode(domain)
		return
	}
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("response encoding: %v", err)
	}
}

func decode(w http.ResponseWriter, r *http.Request, value any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 8<<20)
	reader := json.NewDecoder(r.Body)
	reader.DisallowUnknownFields()
	if err := reader.Decode(value); err != nil {
		return model.Bad("invalid or oversized JSON request")
	}
	var extra any
	if err := reader.Decode(&extra); err != io.EOF {
		return model.Bad("request must contain exactly one JSON value")
	}
	return nil
}

func handleBody[T any](w http.ResponseWriter, r *http.Request, action func(T) (any, error)) {
	var input T
	if err := decode(w, r, &input); err != nil {
		respond(w, nil, err)
		return
	}
	value, err := action(input)
	respond(w, value, err)
}
