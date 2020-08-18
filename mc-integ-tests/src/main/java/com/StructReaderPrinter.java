package com;

import java.io.PrintStream;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.capnproto.StructList;
import org.capnproto.StructReader;

import com.mirth.connect.donkey.model.message.CapnpModel;

public class StructReaderPrinter {
    private boolean headerPrinted;
    private Set<SafeMethod> methodSet;

    private class SafeMethod implements Comparable<SafeMethod>{
        private Method chkMethod;
        private Method valMethod;
        private String name;
        public SafeMethod(Method valMethod) {
            valMethod.setAccessible(true);
            this.valMethod = valMethod;
            this.name = valMethod.getName().substring(3).toLowerCase();
        }
        
        public void setChkMethod(Method chkMethod) {
            chkMethod.setAccessible(true);
            this.chkMethod = chkMethod;
        }
        
        public String eval(Object target) {
            try {
                boolean b = true;
                Object result = null;
                if(chkMethod != null) {
                    b = (Boolean)chkMethod.invoke(target);
                }
                if(b) {
                    result = valMethod.invoke(target);
                }
                
                if(result == null) {
                    result = "";
                }
                else if(result instanceof StructList.Reader) {
                    result = readPreferences((StructList.Reader)result);
                }

                return result.toString();
            }
            catch(Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public int compareTo(SafeMethod o) {
            if(o.name.equals("id")) {
                return 1;
            }

            return name.compareTo(o.name);
        }
    }
    
    private String readPreferences(StructList.Reader r) {
        Iterator<StructReader> itr = r.iterator();
        StringBuilder sb = new StringBuilder();
        while(itr.hasNext()) {
            StructReader sr = itr.next();
            if(sr instanceof CapnpModel.PreferenceEntry.Reader) {
                CapnpModel.PreferenceEntry.Reader entryReader = (CapnpModel.PreferenceEntry.Reader)sr;
                sb.append(entryReader.getKey().toString()).append(':').append(entryReader.getValue().toString());
                sb.append(';');
            }
        }
        
        if(sb.length() > 1) {
            sb.setLength(sb.length()-1);
        }
        
        return sb.toString();
    }

    public void print(Object readerObj, PrintStream target) {
        Set<SafeMethod> set = getMethods(readerObj);
        if(!headerPrinted) {
            for(SafeMethod sm : set) {
                target.print(sm.name + "\t");
            }
            headerPrinted = true;
            target.println();
        }

        for(SafeMethod sm : set) {
            target.print(sm.eval(readerObj).trim() + "\t");
        }
        target.println();
    }
    
    public void reset() {
        headerPrinted = false;
        methodSet = null;
    }

    private Set<SafeMethod> getMethods(Object readerObj) {
        if(methodSet != null) {
            return methodSet;
        }

        Class claz = readerObj.getClass();
        Method[] methods = claz.getDeclaredMethods();
        methodSet = new TreeSet<>();
        for(Method m : methods) {
            String name = m.getName();
            if(name.startsWith("get")) {
                SafeMethod sm = new SafeMethod(m);
                if(!m.getReturnType().isPrimitive()) { // there is a hasXXX method
                    name = "has" + name.substring(3);
                    try {
                        Method hasMethod = claz.getDeclaredMethod(name);
                        sm.setChkMethod(hasMethod);
                    }
                    catch(NoSuchMethodException e) {
                        //e.printStackTrace();
                    }
                }
                
                methodSet.add(sm);
            }
        }
        
        return methodSet;
    }
}
