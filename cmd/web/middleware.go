package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"net/http"
)

func (app *application) recoverPanic(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				w.Header().Set("Connection", "close")
				app.serverError(w, r, fmt.Errorf("%s", err))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func (app *application) logRequest(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var (
			//ip address
			ip = r.RemoteAddr
			//http protocol & version
			proto = r.Proto
			// method = HEAD for e.g.
			method = r.Method
			url    = r.URL.RequestURI()
		)
		app.logger.Info("received request", "ip", ip, "proto", proto, "method", method, "url", url)
		next.ServeHTTP(w, r)
	})
}
func generateNonce() (string, error) {
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(nonce), nil
}

// set common http request headers
func commonHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nonce, err := generateNonce()
		if err != nil {
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}
		//TODO: revert to csp := "default-src 'self'; style-src 'self'; script-src 'self' 'nonce-" + nonce + "'; connect-src 'self' https://storage.googleapis.com"
		csp := "script-src 'self' 'nonce-" + nonce + "'; connect-src 'self' https://storage.googleapis.com"
		w.Header().Set("Content-Security-Policy", csp)
		w.Header().Set("Referrer-Policy", "origin-when-cross-origin")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "deny")
		w.Header().Set("X-XSS-Protection", "0")
		//custom header
		w.Header().Set("Server", "Proprietary-MHK")

		//forward to next handler
		ctx := context.WithValue(r.Context(), "nonce", nonce)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
