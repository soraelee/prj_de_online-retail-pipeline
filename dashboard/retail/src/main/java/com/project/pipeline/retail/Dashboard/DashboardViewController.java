package com.project.pipeline.retail.Dashboard;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping("/main")
public class DashboardViewController {

    @GetMapping("")
    public String setDashBoard(){
        return "dashboard";
    }
}
