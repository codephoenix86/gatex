import http from "k6/http";
import { check } from "k6";

const rate = Number.parseInt(__ENV.RATE || "1000", 10);
const duration = __ENV.DURATION || "10s";
const targetURL = __ENV.TARGET_URL || "http://127.0.0.1:18080/api/items";
const expectGateway = __ENV.EXPECT_GATEWAY === "true";

export const options = {
  discardResponseBodies: true,
  scenarios: {
    steady_traffic: {
      executor: "constant-arrival-rate",
      rate,
      timeUnit: "1s",
      duration,
      preAllocatedVUs: Number.parseInt(__ENV.PREALLOCATED_VUS || "100", 10),
      maxVUs: Number.parseInt(__ENV.MAX_VUS || "300", 10),
    },
  },
  summaryTrendStats: ["avg", "p(50)", "p(95)", "p(99)", "max"],
  thresholds: {
    checks: ["rate==1"],
    dropped_iterations: ["count==0"],
    http_req_failed: ["rate==0"],
  },
};

export default function () {
  const response = http.get(targetURL, {
    headers: {
      "X-Request-ID": `load-${__VU}-${__ITER}`,
    },
  });

  check(response, {
    "status is 200": (result) => result.status === 200,
    "gateway header is present when expected": (result) =>
      !expectGateway || result.headers["X-Gateway"] === "gatex",
  });
}
