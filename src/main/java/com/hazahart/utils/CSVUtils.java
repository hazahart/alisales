package com.hazahart.utils;

public class CSVUtils {

    public static String[] parse(String line) {
        return line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
    }

    public static double parseMoney(String s) {
        return Double.parseDouble(s.replace("$", "").replace("\"", "").replace(",", "").trim());
    }

    public static int parseInteger(String s) {
        return Integer.parseInt(s.replace(",", "").trim());
    }

    public static double parsePercent(String s) {
        return Double.parseDouble(s.replace("%", "").trim());
    }
}