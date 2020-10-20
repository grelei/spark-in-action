package com.java.core;

import org.apache.spark.sql.catalyst.expressions.aggregate.Collect;

import java.util.Collection;
import java.util.List;

public class Example {
    public Example() {
    }

    public static void main(String[] args){

        for(int i=1;i<=9;i++){
            for(int j=1;j<=9;j++){

                if(i==j){
                    System.out.println(i+"*"+j+"="+(i*j));
                    break;
                }
                System.out.print(i+"*"+j+"="+(i*j)+"\t");

            }
        }

        for(int i=1;i<=9;i++){
            for(int j=1;j<=9;j++){

                if(i==j){
                    System.out.println("*");
                    break;
                }
                System.out.print("*");

            }
        }
        Collection collection = null;
        List list  = null ;
    }
}
