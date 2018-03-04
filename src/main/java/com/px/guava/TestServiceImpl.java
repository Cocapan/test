package com.px.guava;

import static com.google.common.base.Preconditions.checkNotNull;

public class TestServiceImpl implements TestService {

    @Override
    public void test(String s) {
        checkNotNull(s);
    }
}
