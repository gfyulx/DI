package com.gfyulx.DI.flink.service;

import java.io.Serializable;

public class TestStruct implements Serializable {

    Integer rid;
    String team;
    String season;
    Integer victory;
    Integer negative;
    Integer ranking;


    Integer getRid(){
        return this.rid;
    }
    String getTeam() {
        return this.team;
    }
    String getSeason(){
        return this.season;
    }
    Integer getVictory(){
        return this.victory;
    }
    Integer getNegative(){
        return this.negative;
    }
    Integer getRanking(){
        return this.ranking;
    }
    public TestStruct(){

    }
    public TestStruct(TestStruct sT){
        new TestStruct();
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

