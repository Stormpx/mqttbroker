package com.stormpx.kit;

public class TopicUtil {


    public static boolean containWildcard(String topicFilter){
        return topicFilter.contains("+")||topicFilter.contains("#");
    }

    public static boolean isShareTopicFilter(String topicFilter){
        if (topicFilter.startsWith("$share/")){
            String[] split = topicFilter.split("/", 3);
            if (!split[1].contains("#")&&!split[1].contains("+")&&split.length==3){
                return true;
            }
        }
        return false;
    }

    public static boolean matches(String filter, String topic){
        if (filter.length()==1){
            if (filter.equals("#")){
                return !topic.startsWith("$");
            }else if (filter.equals("+")){
                return !topic.startsWith("$")&&!topic.contains("/");
            }
        }
        char[] chars = filter.toCharArray();
        char[] ochars = topic.toCharArray();
        for (int i = 0,j=0; i < chars.length||j<ochars.length; i++,j++) {
            if (i>=chars.length) {
                return false;
            }else if (j>=ochars.length){
                if (chars[i]=='/'){
                    return i != chars.length - 1 && chars[i + 1] == '#';
                }else if((chars[i]=='#'||chars[i] == '+')&&i==chars.length-1){
                    return chars[i-1]=='/';
                }
                return false;
            }
            char c = chars[i];

            switch (c){
                case '#':
                    if (i==chars.length-1&&chars[i-1]=='/'){
                        return true;
                    }
                    break;

                case '+':
                    if ((i==0||chars[i-1]=='/')&&(i==chars.length-1||chars[i+1]=='/')){
                        while (j<ochars.length){
                            if (ochars[j]=='/'){
                                j--;
                                break;
                            }else {
                                j++;
                            }
                        }
                        continue;
                    }
                    break;
            }

            if (c!=ochars[j])
                return false;

        }


        return true;
    }
}
