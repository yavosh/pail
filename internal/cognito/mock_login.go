package cognito

import (
	"html/template"
	"net/http"
	"slices"
)

func (b *browserState) mockLogin(w http.ResponseWriter, r *http.Request, p *pool) {
	if !parseBrowserForm(w, r) {
		oauthError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	username := r.PostForm.Get("username")
	cancel := r.PostForm.Get("cancel") == "1" && !r.PostForm.Has("username")
	index := slices.IndexFunc(p.mockProvider.Users, func(user mockUser) bool { return user.Username == username })
	if !cancel && (index < 0 || r.PostForm.Has("cancel")) {
		oauthError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	tx, ok := b.consumeTransaction(w, r, r.PostForm.Get("state"), p.issuer)
	if !ok {
		oauthError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	if cancel {
		oauthRedirect(w, r, tx.redirectURI, tx.appState, "error", "access_denied")
		return
	}
	name := p.mockProvider.Name
	b.grant(w, r, tx, User{
		Subject:  subjectID("mock", p.issuer, name+"\x00"+username),
		Username: name + "_" + username,
		Claims:   p.mockProvider.Users[index].Claims,
	})
}

var mockLoginTemplate = template.Must(template.New("mock-login").Parse(`<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Mock sign-in · pail</title>
  <style>
    :root { color-scheme: light; font: 16px/1.5 system-ui, sans-serif; color: #18263b; background: #eef3f8; }
    * { box-sizing: border-box; }
    body { min-height: 100vh; margin: 0; padding: 1.5rem; display: grid; place-items: center; }
    main { width: min(100%, 28rem); padding: clamp(1.5rem, 5vw, 2.5rem); background: #fff;
      border: 1px solid #d4dfea; border-radius: 1rem; box-shadow: 0 10px 32px #18263b12; }
    .provider { margin: 0; color: #345d86; font-size: .875rem; font-weight: 650; overflow-wrap: anywhere; }
    h1 { margin: .25rem 0 .5rem; font-size: clamp(1.5rem, 5vw, 1.875rem); line-height: 1.2; }
    p { margin: 0 0 1.5rem; color: #45566b; }
    form { display: grid; gap: .625rem; }
    button { padding: .625rem .875rem; border: 1px solid #b6c9dc; border-radius: .5rem; background: #f6f9fc;
      color: #173b61; font: inherit; font-weight: 600; text-align: left; overflow-wrap: anywhere; cursor: pointer; }
    button:hover { background: #e5eff9; border-color: #658ab0; }
    button:focus-visible { outline: 2px solid #2363ac; outline-offset: 2px; }
    .cancel { justify-self: start; margin-top: .5rem; border-color: transparent; background: transparent; }
  </style>
</head>
<body>
  <main>
    <p class="provider">{{.Provider.Name}} · Mock sign-in</p>
    <h1>Choose a test account</h1>
    <p>Sign in as a configured test user. No password is required.</p>
    <form method="post" action="{{.Action}}">
      <input type="hidden" name="state" value="{{.State}}">
      {{range .Provider.Users}}
      <button type="submit" name="username" value="{{.Username}}">{{.Username}}</button>
      {{end}}
      <button class="cancel" type="submit" name="cancel" value="1">Cancel</button>
    </form>
  </main>
</body>
</html>`))

func showMockLogin(w http.ResponseWriter, provider *mockProvider, state string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	// The form redirects to the app's registered callback, which may be on another origin.
	w.Header().Set("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; frame-ancestors 'none'; base-uri 'none'")
	_ = mockLoginTemplate.Execute(w, struct {
		Provider *mockProvider
		Action   string
		State    string
	}{provider, mockLoginPath, state})
}
