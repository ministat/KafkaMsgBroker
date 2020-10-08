package net.local.example;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class FIXDataServlet extends HttpServlet {
    private QueryTable _queryTable;
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        String kuduMasters = config.getInitParameter("kuduMasters");
        String kuduTable = config.getInitParameter("kuduTable");
        _queryTable = new QueryTable(kuduMasters, kuduTable);
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        response.setContentType("text/plain");
        response.setStatus(HttpServletResponse.SC_OK);
        try {
            BufferedWriter writer = new BufferedWriter(
                    new OutputStreamWriter(response.getOutputStream(), "UTF-8"));
            String result = _queryTable.Query();
            writer.write(result);
            writer.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
