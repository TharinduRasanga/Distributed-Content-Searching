package com.distributed.fs.service;

import com.distributed.fs.Util;
import com.distributed.fs.filesystem.FileManager;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;

@Service
public class FileService {
    private final FileManager fileManager;

    public FileService(FileManager fileManager) {
        this.fileManager = fileManager;
    }

    public byte[] getFileContent(String fileName) {
        if (fileManager.containsLocally(fileName)) {
            return Util.createRandomContentToFile().getBytes();
        }
        throw new ResponseStatusException(HttpStatus.NO_CONTENT, "Not Found");
    }
}
