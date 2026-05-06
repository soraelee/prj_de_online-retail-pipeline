jQuery(function () {
    let dailyOrderCancelChart = null;
    let eventRatioChart = null;
    let customerRepeatChart = null;
    let productTopChart = null;

    $("#searchBtn").on("click", function () {
        loadDashboard();
    });

    loadDashboard();

    async function loadDashboard() {
        await Promise.allSettled([
            loadDailyOrderCancelChart(),
            loadEventRatioChart(),
            loadCustomerRepeatChart(),
            loadProductTopChart()
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

    async function loadDailyOrderCancelChart() {
        const params = getPeriodParams();

        try {
            const orderQuery = makeQuery({
                startDate: params.startDate,
                endDate: params.endDate,
                eventType: "order"
            });

            const cancelQuery = makeQuery({
                startDate: params.startDate,
                endDate: params.endDate,
                eventType: "cancel"
            });

            const orderResult = await getJson("/dashboard-api/daily-order-info?" + orderQuery);
            const cancelResult = await getJson("/dashboard-api/daily-order-info?" + cancelQuery);

            const orderRows = orderResult.data || [];
            const cancelRows = cancelResult.data || [];

            const chartData = mergeDailyOrderCancelRows(orderRows, cancelRows);

            renderDailyOrderCancelChart(chartData);
        } catch (e) {
            console.error(e);
            renderEmpty(".dailyOrderCancelGraph", "일별 주문/취소 데이터를 불러오지 못했습니다.");
        }
    }

    function mergeDailyOrderCancelRows(orderRows, cancelRows) {
        const map = {};

        orderRows.forEach(function (row) {
            const date = row.invoice_date;

            if (!map[date]) {
                map[date] = {
                    date: date,
                    orderCount: 0,
                    cancelCount: 0
                };
            }

            map[date].orderCount += Number(row.order_count || 0);
        });

        cancelRows.forEach(function (row) {
            const date = row.invoice_date;

            if (!map[date]) {
                map[date] = {
                    date: date,
                    orderCount: 0,
                    cancelCount: 0
                };
            }

            map[date].cancelCount += Number(row.order_count || 0);
        });

        return Object.values(map).sort(function (a, b) {
            return String(a.date).localeCompare(String(b.date));
        });
    }

    function renderDailyOrderCancelChart(rows) {
        const categories = rows.map(function (row) {
            return formatDateLabel(row.date);
        });

        const orderCounts = rows.map(function (row) {
            return row.orderCount;
        });

        const cancelCounts = rows.map(function (row) {
            return row.cancelCount;
        });

        const options = {
            series: [
                {
                    name: "주문",
                    data: orderCounts
                },
                {
                    name: "취소",
                    data: cancelCounts
                }
            ],
            chart: {
                type: "line",
                height: "100%",
                toolbar: {
                    show: false
                },
                fontFamily: "Pretendard, sans-serif"
            },
            stroke: {
                curve: "smooth",
                width: 2
            },
            markers: {
                size: 3
            },
            colors: ["#6979F7", "#FF842C"],
            xaxis: {
                categories: categories,
                axisBorder: {
                    show: false
                },
                axisTicks: {
                    show: false
                },
                labels: {
                    style: {
                        colors: "#808080"
                    }
                }
            },
            yaxis: {
                min: 0,
                labels: {
                    formatter: function (value) {
                        return parseInt(value || 0, 10);
                    },
                    style: {
                        colors: "#BDBDBD"
                    }
                }
            },
            grid: {
                borderColor: "#E2E2E2"
            },
            legend: {
                position: "top",
                horizontalAlign: "right"
            },
            tooltip: {
                shared: true,
                y: {
                    formatter: function (value) {
                        return formatNumber(value) + "건";
                    }
                }
            }
        };

        if (dailyOrderCancelChart) {
            dailyOrderCancelChart.destroy();
        }

        $(".dailyOrderCancelGraph").empty();

        dailyOrderCancelChart = new ApexCharts(
            document.querySelector(".dailyOrderCancelGraph"),
            options
        );

        dailyOrderCancelChart.render();
    }

    async function loadEventRatioChart() {
        const params = getPeriodParams();

        try {
            const query = makeQuery({
                startDate: params.startDate,
                endDate: params.endDate
            });

            const result = await getJson("/dashboard-api/summary/daily-orders?" + query);
            const rows = result.data || [];
            const summary = rows.length > 0 ? rows[0] : {};

            renderEventRatioChart(summary);
            renderEventRatioSummary(summary);
        } catch (e) {
            console.error(e);
            renderEmpty(".eventRatioGraph", "주문/취소 비율 데이터를 불러오지 못했습니다.");
            renderEventRatioSummary({});
        }
    }

    function renderEventRatioSummary(summary) {
        const totalEventCnt = Number(summary.total_event_cnt || 0);
        const orderCnt = Number(summary.order_cnt || 0);
        const cancelCnt = Number(summary.cancel_cnt || 0);

        $("#totalEventCnt").text(formatNumber(totalEventCnt) + "건");
        $("#orderCnt").text(formatNumber(orderCnt) + "건");
        $("#cancelCnt").text(formatNumber(cancelCnt) + "건");
    }

    function renderEventRatioChart(summary) {
        const orderCnt = Number(summary.order_cnt || 0);
        const cancelCnt = Number(summary.cancel_cnt || 0);

        const options = {
            series: [orderCnt, cancelCnt],
            chart: {
                type: "donut",
                height: "100%",
                toolbar: {
                    show: false
                },
                fontFamily: "Pretendard, sans-serif"
            },
            labels: ["주문", "취소"],
            colors: ["#6979F7", "#FF842C"],
            legend: {
                position: "bottom"
            },
            dataLabels: {
                enabled: true,
                formatter: function (value) {
                    return Number(value || 0).toFixed(1) + "%";
                }
            },
            plotOptions: {
                pie: {
                    donut: {
                        size: "58%",
                        labels: {
                            show: true,
                            total: {
                                show: true,
                                label: "전체",
                                formatter: function () {
                                    return formatNumber(orderCnt + cancelCnt);
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

        if (eventRatioChart) {
            eventRatioChart.destroy();
        }

        $(".eventRatioGraph").empty();

        eventRatioChart = new ApexCharts(
            document.querySelector(".eventRatioGraph"),
            options
        );

        eventRatioChart.render();
    }

    async function loadCustomerRepeatChart() {
        const params = getPeriodParams();

        try {
            const query = makeQuery({
                startDate: params.startDate,
                endDate: params.endDate
            });

            const result = await getJson("/dashboard-api/summary/daily-customer-repeats?" + query);
            const rows = result.data || [];

            renderCustomerRepeatChart(rows);
        } catch (e) {
            console.error(e);
            renderEmpty(".customerRepeatGraph", "일별 고객 재구매율 데이터를 불러오지 못했습니다.");
        }
    }

    function renderCustomerRepeatChart(rows) {
        const categories = rows.map(function (row) {
            return formatDateLabel(row.order_date);
        });

        const repeatRates = rows.map(function (row) {
            return toPercentNumber(row.repeat_customer_rate);
        });

        const options = {
            series: [
                {
                    name: "재구매율",
                    data: repeatRates
                }
            ],
            chart: {
                type: "line",
                height: "100%",
                toolbar: {
                    show: false
                },
                fontFamily: "Pretendard, sans-serif"
            },
            stroke: {
                curve: "smooth",
                width: 2
            },
            markers: {
                size: 3
            },
            colors: ["#21A365"],
            xaxis: {
                categories: categories,
                axisBorder: {
                    show: false
                },
                axisTicks: {
                    show: false
                },
                labels: {
                    style: {
                        colors: "#808080"
                    }
                }
            },
            yaxis: {
                min: 0,
                max: 100,
                labels: {
                    formatter: function (value) {
                        return Number(value || 0).toFixed(0) + "%";
                    },
                    style: {
                        colors: "#BDBDBD"
                    }
                }
            },
            grid: {
                borderColor: "#E2E2E2"
            },
            tooltip: {
                y: {
                    formatter: function (value) {
                        return Number(value || 0).toFixed(2) + "%";
                    }
                }
            }
        };

        if (customerRepeatChart) {
            customerRepeatChart.destroy();
        }

        $(".customerRepeatGraph").empty();

        customerRepeatChart = new ApexCharts(
            document.querySelector(".customerRepeatGraph"),
            options
        );

        customerRepeatChart.render();
    }

    async function loadProductTopChart() {
        const params = getPeriodParams();

        try {
            const query = makeQuery({
                startDate: params.startDate,
                endDate: params.endDate,
                eventType: "order"
            });

            const result = await getJson("/dashboard-api/summary/daily-product-sales?" + query);
            const rows = result.data || [];

            renderProductTopChart(rows);
        } catch (e) {
            console.error(e);
            renderEmpty(".productTopGraph", "상품별 주문 Top 5 데이터를 불러오지 못했습니다.");
        }
    }

    function renderProductTopChart(rows) {
        const chartRows = rows.slice(0, 5);

        const categories = chartRows.map(function (row) {
            return row.product_name || "-";
        });

        const eventCounts = chartRows.map(function (row) {
            return Number(row.event_cnt || 0);
        });

        const options = {
            series: [
                {
                    name: "주문 수",
                    data: eventCounts
                }
            ],
            chart: {
                type: "bar",
                height: "100%",
                toolbar: {
                    show: false
                },
                fontFamily: "Pretendard, sans-serif"
            },
            colors: ["#7FE5D8"],
            plotOptions: {
                bar: {
                    columnWidth: "50%",
                    borderRadius: 2,
                    dataLabels: {
                        position: "top"
                    }
                }
            },
            dataLabels: {
                enabled: true,
                formatter: function (value) {
                    return formatNumber(value);
                },
                offsetY: -22,
                style: {
                    fontSize: "13px",
                    fontWeight: 700,
                    colors: ["#111"]
                }
            },
            xaxis: {
                categories: categories,
                axisBorder: {
                    show: false
                },
                axisTicks: {
                    show: false
                },
                labels: {
                    style: {
                        colors: "#1A1A1A"
                    }
                }
            },
            yaxis: {
                min: 0,
                labels: {
                    formatter: function (value) {
                        return parseInt(value || 0, 10);
                    },
                    style: {
                        colors: "#BDBDBD"
                    }
                }
            },
            grid: {
                borderColor: "#E2E2E2"
            },
            legend: {
                show: false
            },
            tooltip: {
                custom: function ({ dataPointIndex }) {
                    const row = chartRows[dataPointIndex] || {};

                    return ""
                        + "<div style='padding:10px;'>"
                        + "<b>" + safeText(row.product_name) + "</b><br/>"
                        + safeText(row.description) + "<br/>"
                        + "카테고리: " + safeText(row.category) + "<br/>"
                        + "주문 수: " + formatNumber(row.event_cnt) + "건"
                        + "</div>";
                }
            }
        };

        if (productTopChart) {
            productTopChart.destroy();
        }

        $(".productTopGraph").empty();

        productTopChart = new ApexCharts(
            document.querySelector(".productTopGraph"),
            options
        );

        productTopChart.render();
    }

    function renderEmpty(selector, message) {
        if (selector === ".dailyOrderCancelGraph" && dailyOrderCancelChart) {
            dailyOrderCancelChart.destroy();
            dailyOrderCancelChart = null;
        }

        if (selector === ".eventRatioGraph" && eventRatioChart) {
            eventRatioChart.destroy();
            eventRatioChart = null;
        }

        if (selector === ".customerRepeatGraph" && customerRepeatChart) {
            customerRepeatChart.destroy();
            customerRepeatChart = null;
        }

        if (selector === ".productTopGraph" && productTopChart) {
            productTopChart.destroy();
            productTopChart = null;
        }

        $(selector).html("<div class='chartEmpty'>" + message + "</div>");
    }

    function formatNumber(value) {
        return Number(value || 0).toLocaleString();
    }

    function toPercentNumber(value) {
        const number = Number(value || 0);

        if (number <= 1) {
            return Number((number * 100).toFixed(2));
        }

        return Number(number.toFixed(2));
    }

    function formatDateLabel(value) {
        if (!value) {
            return "-";
        }

        return String(value).substring(5, 10);
    }

    function safeText(value) {
        if (value === null || value === undefined || value === "") {
            return "-";
        }

        return String(value);
    }
});