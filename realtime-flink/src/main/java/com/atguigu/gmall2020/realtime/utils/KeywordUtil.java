package com.atguigu.gmall2020.realtime.utils;



import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class KeywordUtil {

    public static void main(String[] args) {
        String text = "Apple iPhoneXSMax (A2104) 256GB 深空灰色 移动联通电信4G手机 双卡双待";
        StringReader sr = new StringReader(text);
        IKSegmenter ik = new IKSegmenter(sr, true);
        Lexeme lex = null;
        while (true) {
            try {
                if (!((lex = ik.next()) != null)) break;
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.print(lex.getLexemeText() + "|");

        }
    }

    public static List<String> analyze(String text){
        StringReader sr = new StringReader(text);
        IKSegmenter ik = new IKSegmenter(sr, true);
        Lexeme lex = null;
        List<String> keywordList=new ArrayList();
        while (true) {
            try {
                if ( (lex = ik.next()) != null ){
                    String lexemeText = lex.getLexemeText();
                    keywordList.add(lexemeText);
                }else{
                    break;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        return  keywordList;
    }
}
