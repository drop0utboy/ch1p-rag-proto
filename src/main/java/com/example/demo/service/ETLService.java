package com.example.demo.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.ai.document.Document;
import org.springframework.ai.document.DocumentReader;
import org.springframework.ai.document.DocumentTransformer;
import org.springframework.ai.reader.JsonReader;
import org.springframework.ai.reader.TextReader;
import org.springframework.ai.reader.jsoup.JsoupDocumentReader;
import org.springframework.ai.reader.jsoup.config.JsoupDocumentReaderConfig;
import org.springframework.ai.reader.pdf.PagePdfDocumentReader;
import org.springframework.ai.reader.tika.TikaDocumentReader;
import org.springframework.ai.transformer.splitter.TokenTextSplitter;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class ETLService {
    @Autowired
    private VectorStore vectorStore;

    public String etlFormFile(String title, String author, MultipartFile attach) throws Exception {
        // E : 추출 (text를 document로 생성)
        List<Document> documents = extractFormFile(attach);
        for (Document doc : documents) {
            Map<String, Object> metadata = doc.getMetadata();
            metadata.put("title", title);
            metadata.put("author", author);
            metadata.put("source", attach.getOriginalFilename());
        }

        // T : 변환 (잘게 쪼개는 과정)
        List<Document> splitted_documents = transform(documents);
        // L : 저장 (VectorStore에 저장)
        vectorStore.add(splitted_documents);

        return "%d개를 %d로 쪼개어서 VectorStore에 저장함.".formatted(documents.size(), splitted_documents.size());
    }

    private List<Document> extractFormFile(MultipartFile attach) throws Exception {
        String fileName = attach.getOriginalFilename();
        String contentType = attach.getContentType();
        log.info("contentType: {}", contentType);

        byte[] bytes = attach.getBytes();
        Resource resource = new ByteArrayResource(bytes);

        List<Document> documents = new ArrayList<>();

        if (contentType.equals("text/plain")) {
            DocumentReader reader = new TextReader(resource);
            documents = reader.read();
            log.info("추출된 Document 수: {}", documents.size());

        } else if (contentType.equals("application/pdf")) {
            DocumentReader reader = new PagePdfDocumentReader(resource);
            documents = reader.read();
            log.info("추출된 Document 수: {}", documents.size());

        } else if (contentType.contains("word")) {
            DocumentReader reader = new TikaDocumentReader(resource);
            documents = reader.read();
            log.info("추출된 Document 수: {}", documents.size());

        }

        return documents;
    }

    private List<Document> transform(List<Document> documents) {
        // ✅ 수정: 자바는 이름있는 인자 없음 → 기본 생성자 사용
        TokenTextSplitter tokenTextSplitter = new TokenTextSplitter(200, 50, 0, 10000, false);
        List<Document> splitted_docs = tokenTextSplitter.apply(documents);
        return splitted_docs;
    }

    public String etlFromHtml(String title, String author, String url) throws Exception {
        Resource resource = new UrlResource(url);

        // E: 추출하기
        JsoupDocumentReader reader = new JsoupDocumentReader(
                resource,
                JsoupDocumentReaderConfig.builder()
                        .charset("UTF-8")
                        .selector("#content")
                        .additionalMetadata(Map.of(
                                "title", title,
                                "author", author,
                                "url", url))
                        .build());

        List<Document> documents = reader.read();
        log.info("추출된 Document 수: {} 개", documents.size());

        // T: 변환하기
        DocumentTransformer transformer = new TokenTextSplitter();
        List<Document> transformedDocuments = transformer.apply(documents);
        log.info("변환된 Document 수: {} 개", transformedDocuments.size());

        // L: 적재하기
        vectorStore.add(transformedDocuments);

        return "html에서 etl완료";

    }

    // ##### JSON의 ETL 과정을 처리하는 메소드 #####
    public String etlFromJson(String url) throws Exception {
        // URL로부터 Resource 얻기
        Resource resource = new UrlResource(url);

        // E: 추출하기
        JsonReader reader = new JsonReader(
                resource,
                jsonMap -> Map.of(
                        "title", jsonMap.get("title"),
                        "author", jsonMap.get("author"),
                        "url", url),
                "date",
                "content");

        List<Document> documents = reader.read();
        log.info("추출된 Document 수: {} 개", documents.size());

        // T: 변환하기
        DocumentTransformer transformer = new TokenTextSplitter();
        List<Document> transformedDocuments = transformer.apply(documents);
        log.info("변환된 Document 수: {} 개", transformedDocuments.size());

        // L: 적재하기
        vectorStore.add(transformedDocuments);

        return "JSON에서 추출-변환-적재 완료 했습니다.";
    }

}