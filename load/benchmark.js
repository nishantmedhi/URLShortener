import http from "k6/http";
import { check } from "k6";

const baseUrl = __ENV.BASE_URL || "http://localhost:8080";
const linkCount = Number(__ENV.LINK_COUNT || 500);
const rate = Number(__ENV.RATE || 10000);

export const options = {
  scenarios: {
    redirects: {
      executor: "constant-arrival-rate",
      rate,
      timeUnit: "1s",
      duration: __ENV.DURATION || "60s",
      preAllocatedVUs: Number(__ENV.PREALLOCATED_VUS || 1000),
      maxVUs: Number(__ENV.MAX_VUS || 5000),
      exec: "redirect",
    },
  },
  thresholds: {
    http_req_failed: ["rate<0.01"],
    http_req_duration: ["p(95)<250", "p(99)<500"],
    checks: ["rate>0.99"],
  },
};

export function setup() {
  const codes = [];
  for (let i = 0; i < linkCount; i += 1) {
    const response = http.post(
      `${baseUrl}/api/v1/links`,
      JSON.stringify({ url: `https://example.com/benchmark/${i}` }),
      { headers: { "Content-Type": "application/json" }, timeout: "10s" },
    );
    if (response.status !== 201) {
      throw new Error(`Link setup failed (${response.status}): ${response.body}`);
    }
    codes.push(response.json("code"));
  }
  return { codes };
}

export function redirect(data) {
  const code = data.codes[Math.floor(Math.random() * data.codes.length)];
  const response = http.get(`${baseUrl}/${code}`, { redirects: 0 });
  check(response, {
    "redirect returns 302": (result) => result.status === 302,
  });
}
