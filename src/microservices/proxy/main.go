package main

import (
	"encoding/json"
	"log"
	"math/rand/v2"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
)

var (
	monolithURL string
	moviesURL   string
)

func main() {
	monolithURL = os.Getenv("MONOLITH_URL")
	if monolithURL == "" {
		log.Fatal("MONOLITH_URL must be specified")
	}
	monolithProxy, err := newReverseProxy(monolithURL)
	if err != nil {
		log.Fatalf("Failed to create reverse proxy for monolith: %v", err)
	}

	moviesURL = os.Getenv("MOVIES_SERVICE_URL")
	if moviesURL == "" {
		log.Fatal("MOVIES_SERVICE_URL must be specified")
	}
	moviesProxy, err := newReverseProxy(moviesURL)
	if err != nil {
		log.Fatalf("Failed to create reverse proxy for movies service: %v", err)
	}
	enableProxy := strings.ToLower(os.Getenv("GRADUAL_MIGRATION")) == "true"

	// Set up HTTP routes
	http.HandleFunc("/api/movies", func(writer http.ResponseWriter, request *http.Request) {
		proxy := monolithProxy
		proxyUrl := monolithURL
		if enableProxy {
			percent, err := strconv.Atoi(os.Getenv("MOVIES_MIGRATION_PERCENT"))
			if err != nil {
				percent = 100
			}

			current := rand.N(100)
			if current < percent {
				proxy = moviesProxy
				proxyUrl = moviesURL
			}
		}
		log.Printf("Routing request to %s\n", proxyUrl)
		proxy.ServeHTTP(writer, request)
	})
	http.HandleFunc("/api/users", func(writer http.ResponseWriter, request *http.Request) {
		monolithProxy.ServeHTTP(writer, request)
	})
	http.HandleFunc("/health", handleHealth)

	// Start server
	port := os.Getenv("PORT")
	if port == "" {
		port = "8000" // Note: Using a different port than the monolith
	}
	log.Printf("Starting proxy microservice on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func newReverseProxy(targetURL string) (*httputil.ReverseProxy, error) {
	url, err := url.Parse(targetURL)
	if err != nil {
		return nil, err
	}

	return httputil.NewSingleHostReverseProxy(url), nil
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}
