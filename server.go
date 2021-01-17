package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/hashicorp/raft"
	"net/http"
	"strings"
)

type Handler struct {
	f        *fsm
	httpAddr string
	server   *http.Server
}

func (h *Handler) StartServer() error {
	m := http.NewServeMux()
	s := &http.Server{
		Addr:    h.httpAddr,
		Handler: m,
	}

	h.server = s

	m.HandleFunc("/", h.ServeHTTP)
	if err := s.ListenAndServe(); err != nil {
		return err
	}

	return nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		h.handleGet(w, r)
	case "POST":
		h.handlePost(w, r)
	case "DELETE":
		h.handleDelete(w, r)
	case "PATCH":
		h.handlePatch(w, r)
	default:
		w.WriteHeader(http.StatusBadRequest)
	}
}

func (h *Handler) handleGet(w http.ResponseWriter, r *http.Request) {
	s := strings.Split(r.URL.Path, "/")
	if len(s) < 2 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	k := s[2]
	v := h.f.Get(k)
	fmt.Fprintf(w, "{Key: %s, Value: %s}\n", k, v)
}

func (h *Handler) handlePost(w http.ResponseWriter, r *http.Request) {
	s := strings.Split(r.URL.Path, "/")
	if s[1] == "join" {
		h.handleJoin(w, r)
		return
	}

	if s[1] == "snapshot" {
		h.handleSnapshot(w)
		return
	}

	kv := map[string]string{}

	d := json.NewDecoder(r.Body)
	if err := d.Decode(&kv); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	for k, v := range kv {
		if err := h.f.Insert(k, v); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			continue
		}
		fmt.Fprintf(w, "Insert: {%s, %s}\n", k, v)
	}
}

func (h *Handler) handleDelete(w http.ResponseWriter, r *http.Request) {
	s := strings.Split(r.URL.Path, "/")

	if s[1] == "remove" {
		h.handleRemove(w, s[2])
		return
	}

	if s[1] == "shutdown" {
		h.handleShutdown(w)
		return
	}

	if len(s) < 2 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	k := s[2]
	if err := h.f.Delete(k); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	fmt.Fprintf(w, "Deleted if existed, Key: %s\n", k)
}

func (h *Handler) handleJoin(w http.ResponseWriter, r *http.Request) {
	j := map[string]string{}

	d := json.NewDecoder(r.Body)
	if err := d.Decode(&j); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	err := h.f.r.AddVoter(raft.ServerID(j["Id"]), raft.ServerAddress(j["Address"]), 0, 0)
	if err.Error() != nil {
		fmt.Fprintf(w, "Attempt to join the cluster failed")
	}
}

func (h *Handler) handleSnapshot(w http.ResponseWriter) {
	e := h.f.r.Snapshot()
	if err := e.Error(); err != nil {
		fmt.Fprint(w, err)
		return
	}
	fmt.Fprint(w, "snapshot created successfully")
}

func (h *Handler) handleRemove(w http.ResponseWriter, Id string) {
	//remove server from the cluster
	err := h.f.r.RemoveServer(raft.ServerID(Id), 0, 0)
	if err.Error() != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Removing server from the cluster failed\n")
	}
}

func (h *Handler) handlePatch(w http.ResponseWriter, r *http.Request) {
	kv := map[string]string{}

	d := json.NewDecoder(r.Body)
	if err := d.Decode(&kv); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	for k, v := range kv {
		if err := h.f.Update(k, v); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, "Updated if existed, key: %s\n", k)
	}
}

func (h *Handler) handleShutdown(w http.ResponseWriter) {
	e := h.f.r.Shutdown()
	if err := e.Error(); err != nil {
		fmt.Fprint(w, err)
		return
	}
	fmt.Fprint(w, "Raft node shutdown successful\n")

	if err := h.shutdownServer(); err != nil {
		fmt.Fprint(w, "HTTP server shutdown failed\n")
	}
	fmt.Fprint(w, "HTTP server shutdown successful\n")
}

func (h *Handler) shutdownServer() error {
	go func() {
		if err := h.server.Shutdown(context.Background()); err != nil {
		}
	}()
	return nil
}
