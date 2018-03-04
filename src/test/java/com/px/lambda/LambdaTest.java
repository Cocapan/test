package com.px.lambda;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * @Authon:panxuan
 * @Description:
 * @Date: Created in 11:00 2018/1/3
 * @Modified By:
 */
public class LambdaTest {

    public Function splitWord(String line){
        line = "or 420 million US dollars";
            List<Integer> costBeforeTax = Arrays.asList(100, 200, 300, 400, 500);
            double bill = costBeforeTax.stream().map((cost) -> cost * 1.12).reduce((sum, cost) -> sum + cost).get();
        return new Function() {
            @Override
            public Object apply(Object o) {
                return null;
            }
        };
    }

    public void dataProcess(Function function){
        function.apply("or 420 million US dollars");
    }

    public static void main(String[] args) {
        new LambdaTest().dataProcess(new Function() {
            @Override
            public Object apply(Object o) {
                Stream<String> stream = Arrays.stream(((String) o).split(" "));
                return stream;
            }
        });
    }

}
