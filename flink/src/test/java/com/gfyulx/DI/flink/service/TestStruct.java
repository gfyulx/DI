package com.gfyulx.DI.flink.service;

import java.io.Serializable;

public class TestStruct implements Serializable {

    Integer rid;
    String team;
    String season;
    Integer victory;
    Integer negative;
    Integer ranking;


    public TestStruct(){

    }
    public TestStruct(Integer rid, String team, String season, Integer victory, Integer negative, Integer ranking) {
        this.rid = rid;
        this.team = team;
        this.season = season;
        this.victory = victory;
        this.negative = negative;
        this.ranking = ranking;
    }
}

