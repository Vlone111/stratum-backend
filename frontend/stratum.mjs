/** Browser ES module. Each client owns one in-memory session. */
export class StratumError extends Error {
  constructor(status, detail) {
    const text = Array.isArray(detail)
      ? detail.map(item => `${item.loc.join(".")} — ${item.msg}`).join("\n")
      : String(detail);
    super(text);
    this.status = status;
    this.detail = detail;
  }
}

export class StratumClient {
  #base;
  #token = null;

  constructor(baseUrl = "http://127.0.0.1:8000") {
    this.#base = baseUrl.replace(/\/$/, "");
  }

  async #request(path, {method = "GET", body, query = {}, signal, session = true} = {}) {
    const url = new URL(this.#base + path);
    for (const [name, value] of Object.entries(query)) {
      if (value !== undefined && value !== null) url.searchParams.set(name, String(value));
    }
    const headers = {};
    if (session && this.#token) headers.Authorization = `Bearer ${this.#token}`;
    if (body !== undefined) headers["Content-Type"] = "application/json";
    const response = await fetch(url, {method, headers, body: JSON.stringify(body), signal});
    if (response.status === 204) return;
    const result = await response.json();
    if (!response.ok) {
      if (session && response.status === 401) this.#token = null;
      throw new StratumError(response.status, result.detail ?? response.statusText);
    }
    return result;
  }

  async login(workspace, login, password) {
    const result = await this.#request("/v1/session", {
      method: "POST", body: {workspace, login, password}, session: false
    });
    this.#token = result.access_token;
    return {workspace: result.workspace, login: result.login, role: result.role, expires: result.expires};
  }

  async logout() {
    try {
      await this.#request("/v1/session", {method: "DELETE"});
    } finally {
      this.#token = null;
    }
  }

  me() { return this.#request("/v1/me"); }
  overview() { return this.#request("/v1/overview"); }
  bindings() { return this.#request("/v1/bindings"); }
  sources(query) { return this.#request("/v1/sources", {query}); }
  createSource(body) { return this.#request("/v1/sources", {method: "POST", body}); }
  updateSource(id, changes) {
    return this.#request(`/v1/sources/${encodeURIComponent(id)}`, {method: "PATCH", body: changes});
  }
  start(id) { return this.#request(`/v1/sources/${encodeURIComponent(id)}/runs`, {method: "POST"}); }
  rotateKey(id) { return this.#request(`/v1/sources/${encodeURIComponent(id)}/key`, {method: "POST"}); }
  runs(query) { return this.#request("/v1/runs", {query}); }
  run(id, signal) { return this.#request(`/v1/runs/${encodeURIComponent(id)}`, {signal}); }
  rejects(id, query) { return this.#request(`/v1/runs/${encodeURIComponent(id)}/rejects`, {query}); }
  retry(id) { return this.#request(`/v1/runs/${encodeURIComponent(id)}/retry`, {method: "POST"}); }
  readings(query) { return this.#request("/v1/readings", {query}); }
  versions(sourceId, eventId, query) {
    return this.#request(`/v1/sources/${encodeURIComponent(sourceId)}/events/${encodeURIComponent(eventId)}/versions`, {query});
  }
}
