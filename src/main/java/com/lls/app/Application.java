package com.lls.app;

import com.lls.app.api.FileSystemCat;

public class Application {

    // hadoop jar word-count-1.0-SNAPSHOT.jar com.lls.app.Application /wordcount/input /wordcount/output

    public static void main(String[] args) throws Exception {
        FileSystemCat cat = new FileSystemCat();
        cat.execute(args);

    }
}
