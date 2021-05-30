package com.stormpx.kit;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

public class FileUtil {

    public static void  create(File file){
        if (!file.getParentFile().exists())
            create(file.getParentFile());
        file.mkdir();
    }

    public static void delete(Path path){
        try {
            Files.walk(path)
//                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::deleteOnExit);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
