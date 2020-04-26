package lat;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;

import java.sql.Time;
import java.util.*;

public class qwe {
    public static void main(String[] args) {

        String jsonArr = "[{\"flag\":\"A2\",\"time\":\"2020-01-05 22:36:55\",\"lat\":\"25.4444444444\",\"lon\":\"444444\"},{\"flag\":\"A3\",\"time\":\"2020-01-05 08:47:00\",\"lat\":\"24444\",\"lon\":\"33333\"},  {\"flag\":\"A1\",\"time\":\"2020-01-06 23:27:25\",\"lat\":\"24444\",,\"lon\":\"2222\"}]";
        JSONArray objects = JSON.parseArray(jsonArr);
//

        List<String> stringCollection = new ArrayList<String>();
        stringCollection.add("ddd2");
        stringCollection.add("aaa2");
        stringCollection.add("bbb1");
        stringCollection.add("aaa1");
        stringCollection.add("bbb3");
        stringCollection.add("ccc");
        stringCollection.add("bbb2");
        stringCollection.add("ddd1");

        stringCollection
                .stream()
                .map(String::toUpperCase)
                .sorted((String a, String b) -> -b.compareTo(a))
                .forEach(System.out::println);

    }

    public static JSONArray jsonArraySort(JSONArray jsonArr, final String sortKey, final boolean is_desc) {
        JSONArray sortedJsonArray = new JSONArray();
        List<JSONObject> jsonValues = new ArrayList<JSONObject>();
        for (int i = 0; i < jsonArr.size(); i++) {
            jsonValues.add(jsonArr.getJSONObject(i));
        }
        Collections.sort(jsonValues, new Comparator<JSONObject>() {

            @Override
            public int compare(JSONObject a, JSONObject b) {
                    String valA = a.getString(sortKey);
                   String valB = b.getString(sortKey);
                System.out.println(valA + "---------" + valB);

                if (is_desc) {
                    return -valA.compareTo(valB);
                } else {
                    return -valB.compareTo(valA);
                }
            }
        });
        for (int i = 0; i < jsonArr.size(); i++) {
            sortedJsonArray.add(jsonValues.get(i));
        }
        return sortedJsonArray;
    }
}
