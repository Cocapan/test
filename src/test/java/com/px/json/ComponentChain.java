package com.px.json;

/**
 * @Authon:panxuan
 * @Description:
 * @Date: Created in 11:00 2018/1/3
 * @Modified By:
 */
class ComponentChain {

    private String component;
    private Chain chain;

    public class Chain {

        private String lastComponent;
        private String nextComponent;

        public String getLastComponent() {
            return lastComponent;
        }

        public void setLastComponent(String lastComponent) {
            this.lastComponent = lastComponent;
        }

        public String getNextComponent() {
            return nextComponent;
        }

        public void setNextComponent(String nextComponent) {
            this.nextComponent = nextComponent;
        }

        @Override
        public String toString() {
            return "Chain{" +
                    "lastComponent='" + lastComponent + '\'' +
                    ", nextComponent='" + nextComponent + '\'' +
                    '}';
        }
    }

    public String getComponent() {
        return component;
    }

    public void setComponent(String component) {
        this.component = component;
    }

    public Chain getChain() {
        return chain;
    }

    public void setChain(Chain chain) {
        this.chain = chain;
    }

    @Override
    public String toString() {
        return "ComponentChain{" +
                "component='" + component + '\'' +
                ", chain=" + chain +
                '}';
    }
}
