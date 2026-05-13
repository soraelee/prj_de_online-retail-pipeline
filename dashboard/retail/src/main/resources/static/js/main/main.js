jQuery(function () {
    const charts = {};

    $("#searchBtn").on("click", function () {
        loadDashboard();
    });

    setDefaultPeriod();
    loadDashboard();

    async function loadDashboard() {
        const params = getPeriodParams();

        await Promise.allSettled([
            loadDailyTrend(params),
            loadHourlyOrderCancel(params),
            loadProductTop(params),
            loadCancelProducts(params),
            loadCountrySales(params),
            loadCustomerRepeatSummary(params)
        ]);
    }

    function getPeriodParams() {
        return {
            startDate: $("#startDate").val(),
            endDate: $("#endDate").val()
        };
    }

    function makeQuery(params) {
        const query = new URLSearchParams();

        Object.keys(params).forEach(function (key) {
            const value = params[key];

            if (value !== null && value !== undefined && value !== "") {
                query.append(key, value);
            }
        });

        return query.toString();
    }

    async function getJson(url) {
        const response = await fetch(url);

        if (!response.ok) {
            const errorText = await response.text();
            throw new Error(response.status + " / " + errorText);
        }

        return await response.json();
    }

    async function loadDailyTrend(params) {
        try {
            const query = makeQuery(params);
            const result = await getJson("/dashboard-api/insights/daily-trend?" + query);
            const rows = result.data || [];

            renderMetricCards(rows);
            renderEventRatioChart(rows);
            renderDailyTrendChart(rows);
        } catch (e) {
            console.error(e);
            renderMetricCards([]);
            renderEmpty("eventRatio", ".eventRatioGraph", "주문/취소 비율 데이터를 불러오지 못했습니다.");
            renderEmpty("dailyTrend", ".dailyTrendGraph", "일별 추이 데이터를 불러오지 못했습니다.");
        }
    }

    function renderMetricCards(rows) {
        const summary = rows.reduce(function (acc, row) {
            acc.totalEventCnt += Number(row.total_event_cnt || 0);
            acc.orderCnt += Number(row.order_cnt || 0);
            acc.cancelCnt += Number(row.cancel_cnt || 0);
            acc.totalSalesAmount += Number(row.total_sales_amount || 0);
            return acc;
        }, {
            totalEventCnt: 0,
            orderCnt: 0,
            cancelCnt: 0,
            totalSalesAmount: 0
        });

        $("#totalEventCnt").text(formatNumber(summary.totalEventCnt) + "건");
        $("#orderCnt").text(formatNumber(summary.orderCnt) + "건");
        $("#cancelCnt").text(formatNumber(summary.cancelCnt) + "건");
        $("#totalSalesAmount").text(formatMoney(summary.totalSalesAmount));
    }

    function renderEventRatioChart(rows) {
        const summary = rows.reduce(function (acc, row) {
            acc.orderCnt += Number(row.order_cnt || 0);
            acc.cancelCnt += Number(row.cancel_cnt || 0);
            return acc;
        }, {
            orderCnt: 0,
            cancelCnt: 0
        });

        const options = {
            series: [summary.orderCnt, summary.cancelCnt],
            chart: baseChart("donut"),
            labels: ["주문", "취소"],
            colors: ["#2F6BFF", "#E44D42"],
            dataLabels: {
                enabled: true,
                formatter: function (value) {
                    return Number(value || 0).toFixed(1) + "%";
                }
            },
            legend: {
                position: "bottom",
                fontSize: "12px"
            },
            plotOptions: {
                pie: {
                    donut: {
                        size: "62%",
                        labels: {
                            show: true,
                            total: {
                                show: true,
                                label: "전체",
                                formatter: function () {
                                    return formatNumber(summary.orderCnt + summary.cancelCnt);
                                }
                            }
                        }
                    }
                }
            },
            tooltip: {
                y: {
                    formatter: function (value) {
                        return formatNumber(value) + "건";
                    }
                }
            }
        };

        renderChart("eventRatio", ".eventRatioGraph", options);
    }

    function renderDailyTrendChart(rows) {
        const categories = rows.map(function (row) {
            return formatDateLabel(row.order_date);
        });

        const options = {
            series: [
                {
                    name: "주문",
                    data: rows.map(function (row) { return Number(row.order_cnt || 0); })
                },
                {
                    name: "취소",
                    data: rows.map(function (row) { return Number(row.cancel_cnt || 0); })
                }
            ],
            chart: baseChart("line"),
            stroke: {
                curve: "smooth",
                width: 3
            },
            markers: {
                size: 2
            },
            colors: ["#2F6BFF", "#E44D42"],
            xaxis: axisOptions(categories),
            yaxis: {
                min: 0,
                labels: {
                    formatter: function (value) { return formatCompact(value); }
                }
            },
            noData: {
                text: "데이터 없음"
            },
            grid: gridOptions(),
            legend: legendOptions(),
            tooltip: {
                shared: true,
                intersect: false,
                y: {
                    formatter: function (value) { return formatNumber(value) + "건"; }
                }
            }
        };

        renderChart("dailyTrend", ".dailyTrendGraph", options);
    }

    async function loadHourlyOrderCancel(params) {
        try {
            const query = makeQuery(params);
            const result = await getJson("/dashboard-api/insights/hourly-order-cancel?" + query);
            const rows = fillHours(result.data || []);

            renderHourlyOrderCancelChart(rows);
        } catch (e) {
            console.error(e);
            renderEmpty("hourlyOrderCancel", ".hourlyOrderCancelGraph", "시간대별 데이터를 불러오지 못했습니다.");
        }
    }

    function renderHourlyOrderCancelChart(rows) {
        const categories = rows.map(function (row) {
            return row.invoice_hour + "시";
        });

        const options = {
            series: [
                {
                    name: "주문",
                    data: rows.map(function (row) { return Number(row.order_cnt || 0); })
                },
                {
                    name: "취소",
                    data: rows.map(function (row) { return Number(row.cancel_cnt || 0); })
                }
            ],
            chart: baseChart("bar"),
            plotOptions: {
                bar: {
                    columnWidth: "58%",
                    borderRadius: 2
                }
            },
            colors: ["#2F6BFF", "#E44D42"],
            dataLabels: {
                enabled: true,
                offsetY: -18,
                style: {
                    fontSize: "11px",
                    fontWeight: 800,
                    colors: ["#1D2433"]
                },
                background: {
                    enabled: false
                },
                formatter: function (value) {
                    return value > 0 ? formatNumber(value) : "";
                }
            },
            xaxis: axisOptions(categories),
            yaxis: {
                min: 0,
                labels: {
                    formatter: function (value) { return formatCompact(value); }
                }
            },
            noData: {
                text: "데이터 없음"
            },
            grid: gridOptions(),
            legend: legendOptions(),
            tooltip: {
                shared: true,
                intersect: false,
                y: {
                    formatter: function (value) { return formatNumber(value) + "건"; }
                }
            }
        };

        renderChart("hourlyOrderCancel", ".hourlyOrderCancelGraph", options);
    }

    async function loadProductTop(params) {
        try {
            const query = makeQuery({
                startDate: params.startDate,
                endDate: params.endDate,
                eventType: "order"
            });
            const result = await getJson("/dashboard-api/summary/daily-product-sales?" + query);

            renderProductTopChart(result.data || []);
        } catch (e) {
            console.error(e);
            renderEmpty("productTop", ".productTopGraph", "상품별 주문 Top 5 데이터를 불러오지 못했습니다.");
        }
    }

    function renderProductTopChart(rows) {
        const chartRows = rows.slice(0, 5);
        const categories = chartRows.map(function (row) {
            return truncateLabel(row.product_name || row.stock_code, 22);
        });

        const options = horizontalBarOptions({
            name: "주문 수",
            rows: chartRows,
            categories: categories,
            values: chartRows.map(function (row) { return Number(row.event_cnt || 0); }),
            color: "#2F6BFF",
            valueFormatter: function (value) { return formatNumber(value) + "건"; },
            tooltipFormatter: function (row) {
                return productTooltip(row, [
                    ["상품명", row.product_name],
                    ["Stock Code", row.stock_code],
                    ["카테고리", row.category],
                    ["주문 수", formatNumber(row.event_cnt) + "건"]
                ]);
            }
        });

        renderChart("productTop", ".productTopGraph", options);
    }

    async function loadCancelProducts(params) {
        try {
            const query = makeQuery({
                startDate: params.startDate,
                endDate: params.endDate,
                limit: 5
            });
            const result = await getJson("/dashboard-api/insights/top-cancel-products?" + query);

            renderCancelProductChart(result.data || []);
        } catch (e) {
            console.error(e);
            renderEmpty("cancelProduct", ".cancelProductGraph", "취소 이상 상품 데이터를 불러오지 못했습니다.");
        }
    }

    function renderCancelProductChart(rows) {
        const chartRows = rows.slice(0, 5);
        const categories = chartRows.map(function (row) {
            return truncateLabel(row.product_name || row.stock_code, 22);
        });

        const options = horizontalBarOptions({
            name: "취소 금액",
            rows: chartRows,
            categories: categories,
            values: chartRows.map(function (row) { return Number(row.cancel_sales_amount || 0); }),
            color: "#E44D42",
            valueFormatter: function (value) { return formatMoney(value); },
            tooltipFormatter: function (row) {
                return productTooltip(row, [
                    ["상품명", row.product_name],
                    ["Stock Code", row.stock_code],
                    ["카테고리", row.category],
                    ["취소량", formatNumber(row.cancel_cnt) + "건"],
                    ["취소 금액", formatMoney(row.cancel_sales_amount)],
                    ["취소율", Number(row.cancel_rate || 0).toFixed(2) + "%"]
                ]);
            }
        });

        renderChart("cancelProduct", ".cancelProductGraph", options);
    }

    async function loadCountrySales(params) {
        try {
            const query = makeQuery({
                startDate: params.startDate,
                endDate: params.endDate,
                limit: 10
            });
            const result = await getJson("/dashboard-api/insights/country-sales?" + query);

            renderCountrySalesChart(result.data || []);
        } catch (e) {
            console.error(e);
            renderEmpty("countrySales", ".countrySalesGraph", "국가별 매출 데이터를 불러오지 못했습니다.");
        }
    }

    function renderCountrySalesChart(rows) {
        const chartRows = rows.slice(0, 8);
        const options = {
            series: chartRows.map(function (row) { return Number(row.total_sales_amount || 0); }),
            chart: baseChart("donut"),
            labels: chartRows.map(function (row) { return row.country || "-"; }),
            colors: ["#2F6BFF", "#30B28C", "#F4B740", "#E44D42", "#6F5BD8", "#00A6A6", "#8B9AAF", "#FF8A5B"],
            dataLabels: {
                enabled: true,
                formatter: function (value) {
                    return Number(value || 0).toFixed(1) + "%";
                }
            },
            legend: {
                position: "bottom",
                fontSize: "12px"
            },
            plotOptions: {
                pie: {
                    donut: {
                        size: "62%",
                        labels: {
                            show: true,
                            total: {
                                show: true,
                                label: "총 순매출",
                                formatter: function () {
                                    const total = chartRows.reduce(function (sum, row) {
                                        return sum + Number(row.total_sales_amount || 0);
                                    }, 0);
                                    return formatCompact(total);
                                }
                            }
                        }
                    }
                }
            },
            tooltip: {
                custom: function ({ seriesIndex }) {
                    const row = chartRows[seriesIndex] || {};
                    return productTooltip(row, [
                        ["국가", row.country],
                        ["송장 수", formatNumber(row.invoice_cnt) + "건"],
                        ["이벤트 수", formatNumber(row.event_cnt) + "건"],
                        ["순매출", formatMoney(row.total_sales_amount)],
                        ["주문 매출", formatMoney(row.order_sales_amount)],
                        ["취소 금액", formatMoney(row.cancel_sales_amount)]
                    ]);
                }
            }
        };

        renderChart("countrySales", ".countrySalesGraph", options);
    }

    async function loadCustomerRepeatSummary(params) {
        try {
            const query = makeQuery(params);
            const result = await getJson("/dashboard-api/insights/customer-repeat-summary?" + query);

            renderCustomerRepeatMetric(result.data || {});
        } catch (e) {
            console.error(e);
            renderCustomerRepeatMetric({});
        }
    }

    function renderCustomerRepeatMetric(summary) {
        const totalCustomerCnt = Number(summary.total_customer_cnt || 0);
        const repeatCustomerCnt = Number(summary.repeat_customer_cnt || 0);
        const repeatCustomerRate = Number(summary.repeat_customer_rate || 0);

        $("#customerRepeatRate").text(repeatCustomerRate.toFixed(2) + "%");
        $("#customerRepeatSummary").text(
            "재구매 고객 " + formatNumber(repeatCustomerCnt) + "명 / 전체 " + formatNumber(totalCustomerCnt) + "명"
        );
    }

    function horizontalBarOptions(config) {
        return {
            series: [
                {
                    name: config.name,
                    data: config.values
                }
            ],
            chart: baseChart("bar"),
            colors: [config.color],
            plotOptions: {
                bar: {
                    horizontal: true,
                    barHeight: "58%",
                    borderRadius: 3,
                    dataLabels: {
                        position: "right"
                    }
                }
            },
            dataLabels: {
                enabled: false,
                formatter: config.valueFormatter,
                style: {
                    fontSize: "12px",
                    fontWeight: 700,
                    colors: ["#1A1A1A"]
                }
            },
            xaxis: {
                categories: config.categories,
                labels: {
                    formatter: function (value) { return formatCompact(value); }
                }
            },
            yaxis: {
                labels: {
                    style: {
                        colors: "#333",
                        fontSize: "12px"
                    }
                }
            },
            grid: gridOptions(),
            legend: { show: false },
            tooltip: {
                custom: function ({ dataPointIndex }) {
                    return config.tooltipFormatter(config.rows[dataPointIndex] || {});
                }
            }
        };
    }

    function renderChart(key, selector, options) {
        if (charts[key]) {
            charts[key].destroy();
        }

        $(selector).empty();
        charts[key] = new ApexCharts(document.querySelector(selector), options);
        charts[key].render();
    }

    function renderEmpty(key, selector, message) {
        if (charts[key]) {
            charts[key].destroy();
            charts[key] = null;
        }

        $(selector).html("<div class='chartEmpty'>" + message + "</div>");
    }

    function setDefaultPeriod() {
        const startInput = $("#startDate");
        const endInput = $("#endDate");

        if (startInput.val() && endInput.val()) {
            return;
        }

        const now = new Date();
        const firstDate = new Date(now.getFullYear(), now.getMonth(), 1);
        const lastDate = new Date(now.getFullYear(), now.getMonth() + 1, 0);

        startInput.val(formatInputDate(firstDate));
        endInput.val(formatInputDate(lastDate));
    }

    function baseChart(type) {
        return {
            type: type,
            height: "100%",
            toolbar: { show: false },
            fontFamily: "Pretendard, sans-serif",
            animations: {
                enabled: true,
                speed: 350
            }
        };
    }

    function axisOptions(categories) {
        return {
            categories: categories,
            axisBorder: { show: false },
            axisTicks: { show: false },
            labels: {
                style: {
                    colors: "#757575"
                }
            }
        };
    }

    function gridOptions() {
        return {
            borderColor: "#E8ECF2",
            strokeDashArray: 3
        };
    }

    function legendOptions() {
        return {
            position: "top",
            horizontalAlign: "right",
            markers: {
                radius: 2
            }
        };
    }

    function fillHours(rows) {
        const map = {};

        rows.forEach(function (row) {
            map[Number(row.invoice_hour)] = row;
        });

        return Array.from({ length: 24 }, function (_, hour) {
            return map[hour] || {
                invoice_hour: hour,
                total_event_cnt: 0,
                order_cnt: 0,
                cancel_cnt: 0,
                cancel_rate: 0
            };
        });
    }

    function productTooltip(row, items) {
        const body = items.map(function (item) {
            return "<div><span>" + safeText(item[0]) + "</span><strong>" + safeText(item[1]) + "</strong></div>";
        }).join("");

        return "<div class='dashboardTooltip'>" + body + "</div>";
    }

    function formatNumber(value) {
        return Number(value || 0).toLocaleString();
    }

    function formatMoney(value) {
        return Number(value || 0).toLocaleString(undefined, {
            maximumFractionDigits: 2
        });
    }

    function formatCompact(value) {
        return formatNumber(Math.round(Number(value || 0)));
    }

    function formatDateLabel(value) {
        if (!value) {
            return "-";
        }

        return String(value).substring(5, 10);
    }

    function formatInputDate(date) {
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, "0");
        const day = String(date.getDate()).padStart(2, "0");

        return year + "-" + month + "-" + day;
    }

    function truncateLabel(value, maxLength) {
        const text = safeText(value);

        if (text.length <= maxLength) {
            return text;
        }

        return text.substring(0, maxLength - 1) + "...";
    }

    function safeText(value) {
        if (value === null || value === undefined || value === "") {
            return "-";
        }

        return String(value);
    }
});
