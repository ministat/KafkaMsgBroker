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
    private int _secondsBefore = 600;
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        String kuduMasters = config.getInitParameter("kuduMasters");
        String kuduTable = config.getInitParameter("kuduTable");
        String sparkMaster = config.getInitParameter("sparkMaster");
        _secondsBefore = Integer.valueOf(config.getInitParameter("timeBefore"));
        System.out.println("kuduMasters: " + kuduMasters + " kuduTable:" + kuduTable + " sparkMaster: " + sparkMaster + " secondsBefore:" + _secondsBefore);
        _queryTable = new QueryTable(kuduMasters, kuduTable, sparkMaster);
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        response.setContentType("text/plain");
        response.setStatus(HttpServletResponse.SC_OK);
        try {
            BufferedWriter writer = new BufferedWriter(
                    new OutputStreamWriter(response.getOutputStream(), "UTF-8"));
            String result = _queryTable.Query(_secondsBefore);
            writer.write(result);
            writer.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void destroy() {
        _queryTable.Stop();
    }
}
