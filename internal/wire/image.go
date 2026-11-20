package wire

import (
	"net/http"
	"strconv"

	"ridgeline/internal/engine"
)

func serveImage(e *engine.Engine, w http.ResponseWriter, r *http.Request) {
	image, err := e.DB.Snapshot()
	if err != nil {
		respond(w, nil, err)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", `attachment; filename="ridgeline.db"`)
	w.Header().Set("X-Ridge-SHA256", image.SHA256)
	w.Header().Set("X-Ridge-Sequence", strconv.FormatUint(image.Sequence, 10))
	w.Header().Set("Content-Length", strconv.Itoa(len(image.Data)))
	w.Write(image.Data)
}
