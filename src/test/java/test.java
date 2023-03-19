import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.beam.sdk.coders.Coder;
import org.bewakoof.analytics.common.utils;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;


public class test {
    private static JsonParser jsonParser = new JsonParser();
    public static void main(String[] args) {
        String input = "{\"common\":{\"context\":{\"page\":{\"path\":\"/men-t-shirts/sizes-XL\",\"search\":\"\",\"title\":\"T-Shirts for Men - Buy Men's T-shirts Online | Bewakoof\",\"url\":\"https://www.bewakoof.com/men-t-shirts/sizes-XL\",\"referrer\":\"\"},\"utm\":{\"source\":\"\",\"medium\":\"\",\"campaign\":\"\"},\"webview\":\"\",\"device_category\":\"desktop\",\"screen\":{\"width\":1366,\"height\":768},\"scroll_depth\":14919},\"ab_id\":\"100\",\"user_id\":\"\",\"anonymous_id\":\"c4381bb0-8a11-11ea-a1cd-479347fc2e10\",\"session_id\":\"0551c0f0-8a1d-11ea-9d94-43cb103151e9\"},\"type\":\"track\",\"event\":\"product_impression\",\"properties\":{\"products\":[{\"product_id\":\"272962\",\"name\":\"Lost Mountains Half Sleeve T-Shirt\",\"price\":399,\"mrp\":449,\"position\":100,\"g_order\":1,\"g_title\":\"T-Shirts For Men\",\"g_position\":100,\"offer_type\":\"best_seller\",\"in_stock\":true,\"labels\":\"\"},{\"product_id\":\"231465\",\"name\":\"World Peace Half Sleeve T-Shirt\",\"price\":399,\"mrp\":449,\"position\":101,\"g_order\":1,\"g_title\":\"T-Shirts For Men\",\"g_position\":101,\"offer_type\":\"best_seller\",\"in_stock\":true,\"labels\":\"\"},{\"product_id\":\"217801\",\"name\":\"Beast Is Unleashed Half Sleeve T-Shirt\",\"price\":349,\"mrp\":399,\"position\":102,\"g_order\":1,\"g_title\":\"T-Shirts For Men\",\"g_position\":102,\"offer_type\":\"factory_outlet_price\",\"in_stock\":true,\"labels\":\"\"},{\"product_id\":\"272979\",\"name\":\"Travel Minimal Pocket Color Block T-Shirt\",\"price\":449,\"mrp\":449,\"position\":103,\"g_order\":1,\"g_title\":\"T-Shirts For Men\",\"g_position\":103,\"offer_type\":\"all_offer\",\"in_stock\":true,\"labels\":\"\"},{\"product_id\":\"242046\",\"name\":\"Bold Red Half Side Panel T-shirt\",\"price\":499,\"mrp\":499,\"position\":104,\"g_order\":1,\"g_title\":\"T-Shirts For Men\",\"g_position\":104,\"offer_type\":\"all_offer\",\"in_stock\":true,\"labels\":\"\"},{\"product_id\":\"271972\",\"name\":\"Attitude Block Color Block T-Shirt\",\"price\":449,\"mrp\":449,\"position\":105,\"g_order\":1,\"g_title\":\"T-Shirts For Men\",\"g_position\":105,\"offer_type\":\"all_offer\",\"in_stock\":true,\"labels\":\"\"}],\"filters\":[{\"type\":\"sizes\",\"value\":\"XL\"}],\"sort\":\"popular\",\"list_id\":\"1\",\"list_name\":\"men-t-shirts\"}}";
        JsonObject jsonObject = (JsonObject) jsonParser.parse(input);
        jsonObject = utils.transformedJson(jsonObject);
        jsonObject = utils.addPageName(jsonObject);
        TableRow row ;//= TableRowJsonCoder.of().decode(jsonObject.toString(), Coder.Context.OUTER)//JsonToRowConverterRaw.jsonToRow2(jsonObject.toString());
        try (InputStream inputStream =
                     new ByteArrayInputStream(jsonObject.toString().getBytes(StandardCharsets.UTF_8))) {
            row = TableRowJsonCoder.of().decode(inputStream, Coder.Context.OUTER);

        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize json to table row: " + jsonObject.toString(), e);
        }

        System.out.println(row);
    }
}
