// Package web embeds the browser client so the server binary is self-contained.
package web

import "embed"

//go:embed index.html app.js wire.js jitterbuffer.js audio.js chart.js style.css copa.html copa.js
var FS embed.FS
