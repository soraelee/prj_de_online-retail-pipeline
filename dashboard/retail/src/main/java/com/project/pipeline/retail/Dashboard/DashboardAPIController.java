package com.project.pipeline.retail.Dashboard;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;

@RestController
@RequestMapping("/dashboard-api")
public class DashboardAPIController {

    private final RestTemplate restTemplate;

    @Value("${retail-api.base-url}")
    private String retailApiBaseUrl;

    public DashboardAPIController(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    /**
     * 일별/시간대별 주문 정보 조회
     * Spring 호출 주소:
     * GET /dashboard-api/daily-order-info?startDate=2025-12-01&endDate=2025-12-31&eventType=order
     *
     * FastAPI 호출 주소:
     * GET /api/v1/daily-order-info?start_date=2025-12-01&end_date=2025-12-31&event_type=order
     */
    @GetMapping("/daily-order-info")
    public ResponseEntity<String> getDailyOrderInfo(
            @RequestParam(required = false) String startDate,
            @RequestParam(required = false) String endDate,
            @RequestParam(required = false) String eventType
    ) {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("start_date", startDate);
        params.put("end_date", endDate);
        params.put("event_type", eventType);

        return callFastApi("/api/v1/daily-order-info", params);
    }

    /**
     * 일별 주문 요약 정보 조회
     * Spring 호출 주소:
     * GET /dashboard-api/summary/daily-orders?startDate=2025-12-01&endDate=2025-12-31
     *
     * FastAPI 호출 주소:
     * GET /api/v1/summary/daily-orders?start_date=2025-12-01&end_date=2025-12-31
     */
    @GetMapping("/summary/daily-orders")
    public ResponseEntity<String> getDailyOrdersSummary(
            @RequestParam(required = false) String startDate,
            @RequestParam(required = false) String endDate
    ) {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("start_date", startDate);
        params.put("end_date", endDate);

        return callFastApi("/api/v1/summary/daily-orders", params);
    }

    /**
     * 일별 상품 판매량 조회
     * Spring 호출 주소:
     * GET /dashboard-api/summary/daily-product-sales?startDate=2025-12-01&endDate=2025-12-31&eventType=order&category=ETC
     *
     * FastAPI 호출 주소:
     * GET /api/v1/summary/daily-product-sales?start_date=2025-12-01&end_date=2025-12-31&event_type=order&category=ETC
     */
    @GetMapping("/summary/daily-product-sales")
    public ResponseEntity<String> getDailyProductSales(
            @RequestParam(required = false) String startDate,
            @RequestParam(required = false) String endDate,
            @RequestParam(required = false) String eventType,
            @RequestParam(required = false) String category
    ) {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("start_date", startDate);
        params.put("end_date", endDate);
        params.put("event_type", eventType);
        params.put("category", category);

        return callFastApi("/api/v1/summary/daily-product-sales", params);
    }

    /**
     * 재구매 고객 요약 조회
     * Spring 호출 주소:
     * GET /dashboard-api/summary/daily-customer-repeats?startDate=2025-12-01&endDate=2025-12-31
     *
     * FastAPI 호출 주소:
     * GET /api/v1/summary/daily-customer-repeats?start_date=2025-12-01&end_date=2025-12-31
     */
    @GetMapping("/summary/daily-customer-repeats")
    public ResponseEntity<String> getDailyCustomerRepeats(
            @RequestParam(required = false) String startDate,
            @RequestParam(required = false) String endDate
    ) {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("start_date", startDate);
        params.put("end_date", endDate);

        return callFastApi("/api/v1/summary/daily-customer-repeats", params);
    }

    @GetMapping("/insights/daily-trend")
    public ResponseEntity<String> getDailyTrend(
            @RequestParam(required = false) String startDate,
            @RequestParam(required = false) String endDate
    ) {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("start_date", startDate);
        params.put("end_date", endDate);

        return callFastApi("/api/v1/insights/daily-trend", params);
    }

    @GetMapping("/insights/hourly-order-cancel")
    public ResponseEntity<String> getHourlyOrderCancel(
            @RequestParam(required = false) String startDate,
            @RequestParam(required = false) String endDate
    ) {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("start_date", startDate);
        params.put("end_date", endDate);

        return callFastApi("/api/v1/insights/hourly-order-cancel", params);
    }

    @GetMapping("/insights/top-cancel-products")
    public ResponseEntity<String> getTopCancelProducts(
            @RequestParam(required = false) String startDate,
            @RequestParam(required = false) String endDate,
            @RequestParam(required = false) Integer limit
    ) {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("start_date", startDate);
        params.put("end_date", endDate);
        params.put("limit", limit);

        return callFastApi("/api/v1/insights/top-cancel-products", params);
    }

    @GetMapping("/insights/country-sales")
    public ResponseEntity<String> getCountrySales(
            @RequestParam(required = false) String startDate,
            @RequestParam(required = false) String endDate,
            @RequestParam(required = false) Integer limit
    ) {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("start_date", startDate);
        params.put("end_date", endDate);
        params.put("limit", limit);

        return callFastApi("/api/v1/insights/country-sales", params);
    }

    @GetMapping("/insights/customer-repeat-summary")
    public ResponseEntity<String> getCustomerRepeatSummary(
            @RequestParam(required = false) String startDate,
            @RequestParam(required = false) String endDate
    ) {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("start_date", startDate);
        params.put("end_date", endDate);

        return callFastApi("/api/v1/insights/customer-repeat-summary", params);
    }

    /**
     * 공통 FastAPI 호출 메서드
     */
    private ResponseEntity<String> callFastApi(String path, Map<String, Object> params) {
        String baseUrl = retailApiBaseUrl;

        if (baseUrl.endsWith("/")) {
            baseUrl = baseUrl.substring(0, baseUrl.length() - 1);
        }

        UriComponentsBuilder builder = UriComponentsBuilder
                .fromUri(URI.create(baseUrl))
                .path(path);

        if (params != null) {
            params.forEach((key, value) -> {
                if (value != null && StringUtils.hasText(String.valueOf(value))) {
                    builder.queryParam(key, value);
                }
            });
        }

        URI uri = builder
                .build()
                .encode()
                .toUri();

        String response = restTemplate.getForObject(uri, String.class);

        return ResponseEntity.ok(response);
    }
}
