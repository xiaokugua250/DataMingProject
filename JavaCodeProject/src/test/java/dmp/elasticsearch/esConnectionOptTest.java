package dmp.elasticsearch;

import com.morty.java.dmp.elasticsearch.esConnectionOpt;
import org.junit.Test;

/**
 * Created by Administrator on 2016/05/12.
 */

public class esConnectionOptTest {
    @Test
    public void esConnectionOptTest(){
        esConnectionOpt opt=new esConnectionOpt();
        opt.getClient(true);
    }

}
