package main

import (
	"encoding/json"
	"fmt"
	"github.com/hashicorp/raft"
	"log"
	"net"
	"net/http"
	"strings"
)

type Handler struct {
	f        *fsm
	httpAddr string
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		h.handleGet(w, r)
	case "POST":
		h.handlePost(w, r)
	case "DELETE":
		h.handleDelete(w, r)
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

	k := s[1]
	v := h.f.Get(k)
	fmt.Fprintf(w, "{Key: %s, Value: %s}\n", k, v)
}

func (h *Handler) handlePost(w http.ResponseWriter, r *http.Request) {
	s := strings.Split(r.URL.Path, "/")
	if s[1] == "join" {
		h.handleJoin(w, r)
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
			fmt.Fprintf(w, "Error while trying to insert  value {%s, %s}\n", k, v)
		}
		fmt.Fprintf(w, "Insert: {%s, %s}\n", k, v)
	}
}

func (h *Handler) handleDelete(w http.ResponseWriter, r *http.Request) {
	s := strings.Split(r.URL.Path, "/")
	if len(s) < 2 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	k := s[1]
	if err := h.f.Delete(k); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	fmt.Fprintf(w, "Deleted if existed, Key: %s\n", k)
}

func (h *Handler) StartServer() {
	l, err := net.Listen("tcp", h.httpAddr)

	if err != nil {
		log.Fatalf("Failed to start http server")
	}

	http.Handle("/", h)
	http.Serve(l, h)
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
