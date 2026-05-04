<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8" %>
<%@ taglib prefix="c" uri="jakarta.tags.core"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/functions" prefix="fn" %>
<!DOCTYPE html>
<html lang="ko">
<head>
	<meta charset="UTF-8" />
    <meta http-equiv="X-UA-Compatible" content="IE=Edge" />
    <meta name="viewport" content="width=device-width, initial-scale=1, minimum-scale=1, maximum-scale=1, user-scalable=yes">
	<meta name="format-detection" content="telephone=no">
    <title>Online-Retail-Dashboard</title>

	<link rel="icon" type="image/x-icon" href="../img/favicon/favicon.ico">

    <link rel="stylesheet" href="../css/plugin/jquery-ui.css"/>
	<link rel="stylesheet" href="../css/common/reset.css"/>
    <link rel="stylesheet" href="../css/common/root.css"/>
	<link rel="stylesheet" href="../css/common/font.css"/>
    <link rel="stylesheet" href="../css/common/common.css"/>
	<link rel="stylesheet" href="../css/common/layout.css"/>
	<link rel="stylesheet" href="../css/common/popup.css"/>
	<link rel="stylesheet" href="../css/main/main.css"/>

    <script type="text/javascript" src="../js/plugin/jquery-3.7.1.min.js"></script>
	<script type="text/javascript" src="../js/plugin/apexcharts.min.js"></script>
	<script type="text/javascript" src="../js/plugin/jquery-ui.js"></script>
    <script type="text/javascript" src="../js/common/calendar.js"></script>
	<script type="text/javascript" src="../js/common/ux_common.js"></script>
	<script type="text/javascript" src="../js/main/main.js"></script>


</head>
<body>
	<div class="wrapper">
		<nav>
			<div class="side-box">
				<h1><a href="index.html"><img src="../img/logo/logo.png"></a></h1>
				<ul class="first-depth">

				</ul>
				<div class="account-box">
					<div class="ico"><img src="../img/icon/account_btn_thumb.png" alt="계정관리"></div>
					<ul>
						<li><span>암호변경</span></li>
						<li><span>로그아웃</span></li>
					</ul>
				</div>
			</div>

		</nav>
		<section>
            <div class="pageTit dashboardTitle">
                <h2>Retail Pipeline Dashboard</h2>

                <div class="period">
                    <input type="date" id="startDate" value="2025-12-01">
                    <span>~</span>
                    <input type="date" id="endDate" value="2025-12-31">
                    <button type="button" id="searchBtn">조회</button>
                </div>
            </div>

            <div class="dashboardGrid">
                <div class="graphBox dashboardChartBox dailyOrderCancelBox">
                    <div class="tit">일별 주문/취소량</div>
                    <div class="graph">
                        <div class="dailyOrderCancelGraph"></div>
                    </div>
                </div>

                <div class="graphBox dashboardChartBox eventRatioBox">
                    <div class="tit">전체 이벤트 기준 주문 / 취소 비율</div>
                    <div class="graph ratioGraphWrap">
                        <div class="eventRatioGraph"></div>
                        <ul class="ratioSummary">
                            <li>
                                <span class="label">전체</span>
                                <strong id="totalEventCnt">0건</strong>
                            </li>
                            <li>
                                <span class="label">주문</span>
                                <strong id="orderCnt">0건</strong>
                            </li>
                            <li>
                                <span class="label">취소</span>
                                <strong id="cancelCnt">0건</strong>
                            </li>
                        </ul>
                    </div>
                </div>

                <div class="graphBox dashboardChartBox customerRepeatBox">
                    <div class="tit">일별 고객 재처리율</div>
                    <div class="graph">
                        <div class="customerRepeatGraph"></div>
                    </div>
                </div>

                <div class="graphBox dashboardChartBox productTopBox">
                    <div class="tit">상품별 주문 Top 5</div>
                    <div class="graph">
                        <div class="productTopGraph"></div>
                    </div>
                </div>
            </div>
        </section>
	</div>
</body>
</html>


