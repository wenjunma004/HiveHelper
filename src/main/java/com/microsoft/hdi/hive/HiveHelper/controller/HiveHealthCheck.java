package com.microsoft.hdi.hive.HiveHelper.controller;

import com.microsoft.hdi.hive.HiveHelper.HiveResponse;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.tools.HiveSchemaHelper;
import org.apache.hadoop.hive.metastore.tools.MetastoreSchemaTool;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.*;
import java.util.Map;

@Controller
public class HiveHealthCheck {
    String userName;
    String password;
    String url;
    String driver;
    String dbType;

    String DBUser;
    String DBUserPassword ;
    String HiveSchemaName;
    Configuration conf;





    public HiveHealthCheck() throws Exception {
        //System.setProperty("hadoop.home.dir", "C:\\Users\\wenjm\\tool");
        String hiveHome = System.getenv("HIVE_HOME");
        System.out.println("HIVE_HOME:"+hiveHome);
        MetastoreSchemaTool.homeDir =hiveHome;
       // System.setProperty("HIVE_HOME", "/home/wenjm/poc/HiveHelper/apache-hive-3.1.2-bin");
        //MetastoreSchemaTool.homeDir ="/home/wenjm/poc/HiveHelper/apache-hive-3.1.2-bin";
        conf = MetastoreConf.newMetastoreConf();
        try {
            userName = HiveSchemaHelper.getValidConfVar(MetastoreConf.ConfVars.CONNECTION_USER_NAME, conf);
            password = HiveSchemaHelper.getValidConfVar(MetastoreConf.ConfVars.PWD, conf);
            url = HiveSchemaHelper.getValidConfVar(MetastoreConf.ConfVars.CONNECT_URL_KEY, conf);
            driver = HiveSchemaHelper.getValidConfVar(MetastoreConf.ConfVars.CONNECTION_DRIVER, conf);
            if("com.mysql.jdbc.Driver".equals(driver)){
                dbType = "mysql";
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        /**
         * read from security config map
         */
        DBUser = "root";
        DBUserPassword = "root";
        HiveSchemaName = "hive";

        initMetastoreEnv();


    }

    private HiveResponse initMetastoreEnv(){
        HiveResponse finalResponse = new HiveResponse(0,"OK");
        System.out.println("------- check hive db status --- ");
        HiveResponse response = countMetaStoreTables();
        if (response.getCode() != 0){
            System.out.println("------- let create DB start ---");
            HiveResponse createDBRes = createHiveDB();
            System.out.println("createDBRes :" + createDBRes);
            System.out.println("------- create DB end");
            if(createDBRes.getCode() == 0){
                System.out.println("------- let init Schema ---");
                HiveResponse initSchemaDBRes = initSchema();
                System.out.println("initSchemaDBRes :" + initSchemaDBRes);
                System.out.println("------- init Schema DB end");
                if(initSchemaDBRes.getCode()!= 0){
                    finalResponse.setCode(initSchemaDBRes.getCode());
                    finalResponse.setMsg("init Schema fail");
                }
            }else{
                finalResponse.setCode(createDBRes.getCode());
                finalResponse.setMsg("Create DB fail");
            }

        }else{
            finalResponse.setCode(response.getCode());
            finalResponse.setMsg("Hive Metastore existed");
            System.out.println("Hive Metastore existed  NO need init !!");
        }
        return finalResponse;
    }

    @RequestMapping("/createHiveDB")
    @ResponseBody
    private HiveResponse createHiveDB() {
        String[] urlArrays = url.split("/");
        if(urlArrays != null && urlArrays.length >3){
            String dbURL = urlArrays[0] + "/" + urlArrays[1] +"/" + urlArrays[2];
            System.out.println("dbURL:" + dbURL);
            System.out.println("create DB with schame Name:" + HiveSchemaName);
            String[] arg = {"schemaTool","-createUser","-dbType",dbType,"-hiveUser",userName,"-hivePassword",
                    password,"-hiveDb",HiveSchemaName, "-userName",DBUser,"-passWord",DBUserPassword,"-url",dbURL};
            int result = MetastoreSchemaTool.run(arg);
            return new HiveResponse(result,"check log to get more detail");
        }else{
            return new HiveResponse(-1,"can not get db connection URL");
        }


    }

    @RequestMapping("/checkHive")
    @ResponseBody
    public HiveResponse checkHive(){
        return new HiveResponse(0,"OK");
    }

    @RequestMapping("/checkHiveMetaStore")
    @ResponseBody
    public HiveResponse checkHiveMetaStore(){
        return new HiveResponse(0,"OK");
    }

    @RequestMapping("/countMetaStoreTables")
    @ResponseBody
    public HiveResponse countMetaStoreTables(){
        int numberOfTable = 0;
        try {
            Connection conn = HiveSchemaHelper.getConnectionToMetastore(userName,password,url,driver,false,conf,HiveSchemaName);
            String SQL = "show tables";
            Statement stmt = conn.createStatement( );
            ResultSet rs = stmt.executeQuery(SQL);
            while(rs.next()){
                String tableName = rs.getString(1);
                System.out.println("table name: " + tableName);
                numberOfTable++;
            }
        } catch (HiveMetaException e) {
            e.printStackTrace();
            return new HiveResponse(-1,e.getMessage());
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            return new HiveResponse(-1,throwables.getMessage());
        }

        return new HiveResponse(0,"there are "+ numberOfTable + " tables");
    }
    @RequestMapping("/initSchema")
    @ResponseBody
    private HiveResponse initSchema() {
        String[] arg = {"schemaTool","-dbType","mysql","--initSchema"};
        int result = MetastoreSchemaTool.run(arg);
        return new HiveResponse(result,"check log to get more detail");
    }



}
