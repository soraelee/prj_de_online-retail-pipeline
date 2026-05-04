jQuery(function(){
    //년/월/일
    $( "._calendar" ).datepicker({
        dateFormat: 'yy-mm-dd',	
        todayHighlight :true,
        autoclose : true,
        currentText: '오늘', 
        closeText: "닫기",  
        monthNames: ['1월', '2월', '3월', '4월', '5월', '6월', '7월', '8월', '9월', '10월', '11월', '12월'],	
        monthNamesShort: ['1월', '2월', '3월', '4월', '5월', '6월', '7월', '8월', '9월', '10월', '11월', '12월'],
        dayNames: ['일', '월', '화', '수', '목', '금', '토'],
        dayNamesShort: ['일', '월', '화', '수', '목', '금', '토'],	
        dayNamesMin: ['일', '월', '화', '수', '목', '금', '토'],	
        showMonthAfterYear: true,	
        yearSuffix: '년',
        ignoreReadonly: true
    });
    $( "._calendar").datepicker('setDate', 'today'); 
    $( "._calendar.reset").datepicker('setDate', ''); 
    $("._calendar.reserve").datepicker("option", {minDate: 0,});
    $("._calendar.search").datepicker("option", {maxDate: 0,});
});//jQuery
    