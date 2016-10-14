package dmp.elasticsearch;

import com.morty.java.dmp.elasticsearch.EsConnectionOpt;
import org.junit.Test;

/**
 * Created by Administrator on 2016/05/12.
 */
public class esConnectionOptTest {
    @Test
    public void esConnectionOptTest() {
        EsConnectionOpt opt = new EsConnectionOpt();

        opt.getClient(true);
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
