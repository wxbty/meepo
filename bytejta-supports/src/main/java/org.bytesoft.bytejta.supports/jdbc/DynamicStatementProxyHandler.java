package org.bytesoft.bytejta.supports.jdbc;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

public class DynamicStatementProxyHandler implements InvocationHandler {

    private Object realObject;


    public DynamicStatementProxyHandler(Object realObject) {
        this.realObject = realObject;
    }

    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

        //代理扩展逻辑
        if ("nativeSQL".equals(method.getName()) || "executeUpdate".equals(method.getName())
                || "prepareStatement".equals(method.getName()) || "prepareCall".equals(method.getName())) {
//            System.out.println("before proxy get sql = "+sql);
//            List<String>  waitBackSqlList = new ArrayList<>();
//            waitBackSqlList.add(sql);
//            Statement st = (Statement)realObject;
//            Connection conn = st.getConnection();
//            String backInfo = SqlpraserUtils.handleRollBack(waitBackSqlList,conn,st);

        }

        return method.invoke(realObject, args);
    }

}
