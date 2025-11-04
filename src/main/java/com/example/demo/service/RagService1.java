package com.example.demo.service;

import java.util.List;
import java.util.Map;

import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.client.advisor.SimpleLoggerAdvisor;
import org.springframework.ai.chat.client.advisor.vectorstore.QuestionAnswerAdvisor;
import org.springframework.ai.document.Document;
import org.springframework.ai.document.DocumentReader;
import org.springframework.ai.reader.JsonReader;
import org.springframework.ai.reader.TextReader;
import org.springframework.ai.reader.jsoup.JsoupDocumentReader;
import org.springframework.ai.reader.jsoup.config.JsoupDocumentReaderConfig;
import org.springframework.ai.reader.pdf.PagePdfDocumentReader;
import org.springframework.ai.reader.tika.TikaDocumentReader;
import org.springframework.ai.transformer.splitter.TokenTextSplitter;
import org.springframework.ai.vectorstore.SearchRequest;
import org.springframework.ai.vectorstore.SearchRequest.Builder;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.Ordered;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class RagService1 {
    private ChatClient chatClient;

    @Autowired
    private VectorStore vectorStore;

    @Autowired
    JdbcTemplate jdbcTemplate;


    public RagService1(ChatClient.Builder chatClientBuilder) {
        this.chatClient = chatClientBuilder.build();
    }

    public void clearVectorStore() {
        jdbcTemplate.update("TRUNCATE TABLE vector_store");
    }

    public void ragEtl(MultipartFile attach, String source, int chunkSize, int minChunkSizeChars) throws Exception {
        // 추출
        Resource resource = new ByteArrayResource(attach.getBytes()) {
            @Override public String getFilename() {
                return attach.getOriginalFilename() != null ? attach.getOriginalFilename() : "upload.bin";
            }
        };
        DocumentReader reader = pickReader(attach, resource);

        
        List<Document> documents = reader.read();

        
        // 메타데이터
        for (Document doc : documents) {
            doc.getMetadata().put("source", source);
            doc.getMetadata().put("filename", attach.getOriginalFilename());
            doc.getMetadata().put("contentType", attach.getContentType());
        }

        // 쪼개기
        TokenTextSplitter splitter = new TokenTextSplitter(chunkSize, minChunkSizeChars, 0, 10000, true);
        documents = splitter.apply(documents);

        // 저장하기
        vectorStore.add(documents);

    }

    /**
     * 업로드 파일의 contentType/확장자에 맞춰 적절한 DocumentReader 반환
     */
    private DocumentReader pickReader(MultipartFile attach, Resource resource) {
        final String name = (attach.getOriginalFilename() == null ? "" : attach.getOriginalFilename().toLowerCase());
        final String ct   = (attach.getContentType() == null ? "" : attach.getContentType().toLowerCase());

        // ----- TEXT -----
        if (ct.startsWith("text/plain") || name.endsWith(".txt")) {
            return new TextReader(resource);
        }

        // ----- PDF (페이지 단위) -----
        if (ct.startsWith("application/pdf") || name.endsWith(".pdf")) {
            return new PagePdfDocumentReader(resource);
        }

        // ----- WORD (doc/docx) -----
        if (ct.contains("wordprocessingml") || ct.contains("msword") || name.endsWith(".doc") || name.endsWith(".docx")) {
            return new TikaDocumentReader(resource); // 워드는 Tika가 안정적
        }

        // ----- CSV -----
        if (ct.startsWith("text/csv") || name.endsWith(".csv")) {
            return new TikaDocumentReader(resource); // 행 단위 처리는 T단계에서
        }

        // ----- JSON -----
        if (ct.startsWith("application/json") || name.endsWith(".json")) {
            // (resource, 메타데이터매퍼, 본문필드명) — 본문 필드는 실제 JSON 구조에 맞게 교체
            return new JsonReader(
                resource,
                (Map<String, Object> jsonMap) -> Map.of(
                    "filename", name,
                    "contentType", ct
                ),
                "content" // ← 임베딩할 본문 필드명 (예: "content"/"text"/"body")
            );
        }

        // ----- XML -----
        if (ct.startsWith("application/xml") || ct.startsWith("text/xml") || name.endsWith(".xml")) {
            return new TikaDocumentReader(resource);
        }

        // ----- HTML -----
        if (ct.startsWith("text/html") || name.endsWith(".html") || name.endsWith(".htm")) {
            // 선택자 필요 시 조정
            return new JsoupDocumentReader(
                resource,
                JsoupDocumentReaderConfig.builder()
                    .charset("UTF-8")
                    .selector("body")
                    .build()
            );
        }

        // ----- 폴백(기타 포맷 전부) -----
        return new TikaDocumentReader(resource);
    }


    public String ragChat(String question, double score, String source) {

        Builder builder = SearchRequest.builder()
            .similarityThreshold(score)
            .topK(3);

        if(source != null && !source.equals("")) {
            builder.filterExpression("source == '%s'".formatted(source));
        }

        SearchRequest searchRequest = builder.build();

        QuestionAnswerAdvisor advisor = QuestionAnswerAdvisor.builder(vectorStore)
            .searchRequest(searchRequest)
            .build();

        String answer = chatClient.prompt()
            .user(question)
            .advisors(advisor, new SimpleLoggerAdvisor(Ordered.LOWEST_PRECEDENCE -1))
            .call()
            .content();

        return answer;
    }

}
