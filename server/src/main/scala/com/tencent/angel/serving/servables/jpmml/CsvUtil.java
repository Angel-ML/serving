/*
 * Copyright (c) 2013 University of Tartu
 */
package com.tencent.angel.serving.servables.jpmml;

import java.io.*;
import java.util.*;

public class CsvUtil {

    private CsvUtil() {
    }

    static public Table readTable(File file) throws IOException {
        return readTable(file, null);
    }

    static public Table readTable(File file, String separator) throws IOException {
        Table table = new Table();

        BufferedReader reader = new BufferedReader(file != null ? new FileReader(file) : new InputStreamReader(System.in));

        try {
            while (true) {
                String line = reader.readLine();
                if (line == null) {
                    break;
                } // End if

                if (separator == null) {
                    separator = getSeparator(line);
                }

                table.add(CsvUtil.parseLine(line, separator));
            }
        } finally {
            reader.close();
        }

        table.setSeparator(separator);

        return table;
    }

    static public void writeTable(Table table, File file) throws IOException {
        BufferedWriter writer = new BufferedWriter(file != null ? new FileWriter(file) : new OutputStreamWriter(System.out));
        try {
            String terminator = "";

            for (List<String> row : table) {
                StringBuffer sb = new StringBuffer();

                sb.append(terminator);
                terminator = "\n";

                String separator = "";

                for (int i = 0; i < row.size(); i++) {
                    sb.append(separator);
                    separator = table.getSeparator();

                    sb.append(row.get(i));
                }

                writer.write(sb.toString());
            }

            writer.flush();
        } finally {
            writer.close();
        }
    }

    static private String getSeparator(String line) {

        if ((line.split(";")).length > 1) {
            return ";";
        } else if ((line.split(",")).length > 1) {
            return ",";
        }

        throw new RuntimeException();
    }

    static public List<String> parseLine(String line, String separator) {
        List<String> result = new ArrayList<String>();

        String[] cells = line.split(separator);
        for (String cell : cells) {

            // Remove quotation marks, if any
            cell = stripQuotes(cell, "\"");
            cell = stripQuotes(cell, "\'");

            // Standardize decimal marks to Full Stop (US)
            if (!(",").equals(separator)) {
                cell = cell.replace(',', '.');
            }

            result.add(cell);
        }

        return result;
    }

    static private String stripQuotes(String string, String quote) {

        if (string.startsWith(quote) && string.endsWith(quote)) {
            string = string.substring(quote.length(), string.length() - quote.length());
        }

        return string;
    }

    static public boolean isMissing(String string) {
        return string == null || ("").equals(string) || ("NA").equals(string) || ("N/A").equals(string);
    }

    static public class Table extends ArrayList<List<String>> {

        private String separator = null;


        public String getSeparator() {
            return this.separator;
        }

        public void setSeparator(String separator) {
            this.separator = separator;
        }
    }
}