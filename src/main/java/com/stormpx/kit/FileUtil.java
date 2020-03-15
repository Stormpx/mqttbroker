package com.stormpx.kit;

import java.io.File;

public class FileUtil {

    public static void  create(File file){
        if (!file.getParentFile().exists())
            create(file.getParentFile());
        file.mkdir();
    }
}
