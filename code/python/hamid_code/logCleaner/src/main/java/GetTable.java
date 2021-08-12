import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.util.ArrayList;

public class GetTable {
    public static String getPathToSave(String table, int counter) {
        return "/home/hamid/skyTable/" + table + "/" + counter + ".csv";
    }

    public static void main(String[] args) throws IOException {
        String GET_URL = "http://skyserver.sdss.org/dr16/en/tools/search/x_results.aspx?searchtool=SQL&TaskName=Skyserver.Search.SQL&syntax=NoSyntax&ReturnHtml=true&";
        ArrayList<String> tableNames = new ArrayList<String>();
        //    tableNames.add("PhotoObjAll"); //101
        //    tableNames.add("GalaxyTag"); //101
        //    tableNames.add("SpecObjAll"); //101
        //    tableNames.add("star");  //101
        //    tableNames.add("photoprimary"); //101
        //    tableNames.add("Galaxy"); //101
        //    tableNames.add("SpecObj"); //101
        //    tableNames.add("photoObj"); //101
        tableNames.add("Field"); //5999
        tableNames.add("FIRST"); //5999
        tableNames.add("SpecPhoto"); //5999
        tableNames.add("SpecPhotoAll"); //5999
        tableNames.add("sppparams"); //5999
        tableNames.add("wise_xmatch"); //5999
        tableNames.add("emissionlinesport"); //5999
        tableNames.add("galspecline"); //5999
        tableNames.add("photoz"); //5999
        //    tableNames.add("wise_allsky"); // 13999
        //    tableNames.add("zoospec");   //13999
        //    tableNames.add("zoonospec");  //13999
        //    tableNames.add("propermotions"); //5999
        //    tableNames.add("stellarmassstarformingport"); //13999
        //    tableNames.add("sdssebossfirefly"); //13999
        //    tableNames.add("spplines"); //13999
        //    tableNames.add("neighbors"); //-1
        //    tableNames.add("specline"); //-1
        //    tableNames.add("stellarmasspcawisc"); //-1
        //    tableNames.add("photoprofile"); //-1
        //    tableNames.add("masses"); //-1
        //    tableNames.add("twomass"); //-1
        //    tableNames.add("photoprofile"); //-1
        //counter<9000=>8999
        //counter<11000=>10999
        //counter<13000=>12999
        //counter<14000=>13999
        for (int counter = 14000; counter <= 14000; counter++) {
            for (int i = 0; i < tableNames.size(); i++) {
                String PARAMS = "cmd=SELECT+*+FROM+" + tableNames.get(i) + "+WHERE+objID+%25+99999>12999+and+objID+%25+99999<" + counter + "&format=csv&tableName=";
                //String PARAMS="cmd=SELECT+*+FROM+" + tableNames.get(i) + "+WHERE+objID+%25+99999=" + counter + "&format=csv&tableName=";
                if (tableNames.get(i).equals("Field"))
                    PARAMS = "cmd=SELECT+*+FROM+" + tableNames.get(i) + "+WHERE+fieldID+%25+99999>12999+and+fieldID+%25+99999<" + counter + "&format=csv&tableName=";
                //PARAMS="cmd=SELECT+*+FROM+" + tableNames.get(i) + "+WHERE+fieldID+%25+99999=" + counter + "&format=csv&tableName=";
                if (tableNames.get(i).equals("SpecObjAll") || tableNames.get(i).equals("specObjAll") || tableNames.get(i).equals("SpecObj")
                        || tableNames.get(i).equals("specObj") || tableNames.get(i).equals("SpecPhoto") || tableNames.get(i).equals("emissionlinesport")
                        || tableNames.get(i).equals("galspecline") || tableNames.get(i).equals("zoospec") || tableNames.get(i).equals("zoonospec") || tableNames.get(i).equals("stellarmassstarformingport"))
                    PARAMS = "cmd=SELECT+*+FROM+" + tableNames.get(i) + "+WHERE+specObjID+%25+99999>12999+and+specObjID+%25+99999<" + counter + "&format=csv&tableName=";
                //PARAMS="cmd=SELECT+*+FROM+" + tableNames.get(i) + "+WHERE+specObjID+%25+99999=" + counter + "&format=csv&tableName=";
                if (tableNames.get(i).equals("sppparams") || tableNames.get(i).equals("sdssebossfirefly") || tableNames.get(i).equals("spplines"))
                    PARAMS = "cmd=SELECT+*+FROM+" + tableNames.get(i) + "+WHERE+SPECOBJID+%25+99999>12999+and+SPECOBJID+%25+99999<" + counter + "&format=csv&tableName=";
                //PARAMS="cmd=SELECT+*+FROM+" + tableNames.get(i) + "+WHERE+SPECOBJID+%25+99999=" + counter + "&format=csv&tableName=";
                if (tableNames.get(i).equals("wise_xmatch"))
                    PARAMS = "cmd=SELECT+*+FROM+" + tableNames.get(i) + "+WHERE+sdss_objid+%25+99999>12999+and+sdss_objid+%25+99999<" + counter + "&format=csv&tableName=";
                //PARAMS="cmd=SELECT+*+FROM+" + tableNames.get(i) + "+WHERE+sdss_objid+%25+99999=" + counter + "&format=csv&tableName=";
                if (tableNames.get(i).equals("wise_allsky"))
                    PARAMS = "cmd=SELECT+*+FROM+" + tableNames.get(i) + "+WHERE+cntr+%25+99999>12999+and+cntr+%25+99999<" + counter + "&format=csv&tableName=";
                //PARAMS="cmd=SELECT+*+FROM+" + tableNames.get(i) + "+WHERE+cntr+%25+99999=" + counter + "&format=csv&tableName=";

                URL url = new URL(GET_URL + PARAMS);
                URLConnection con2 = url.openConnection();
                InputStream is = con2.getInputStream();

                BufferedReader br = new BufferedReader(new InputStreamReader(is));

                String line = null;
                File file = new File(getPathToSave(tableNames.get(i), counter));
                Files.deleteIfExists(file.toPath());
                PrintWriter out = new PrintWriter(getPathToSave(tableNames.get(i), counter));
                br.readLine();
                br.readLine();
                while ((line = br.readLine()) != null) {
                    out.println(line);
                }

                br.close();
                out.close();
            }
        }
    }
}
