package com.taskmesh.mcp.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.taskmesh.mcp.tools.JobManagementTools;
import org.springframework.ai.tool.ToolCallbackProvider;
import org.springframework.ai.tool.method.MethodToolCallbackProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

@Configuration
public class McpConfig {

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .disable(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    /**
     * Register JobManagementTools as MCP tool callbacks.
     * Spring AI MCP server auto-discovers ToolCallbackProvider beans
     * and exposes their methods as MCP tools over SSE at /sse.
     */
    @Bean
    public ToolCallbackProvider jobManagementToolCallbacks(JobManagementTools tools) {
        return MethodToolCallbackProvider.builder()
            .toolObjects(tools)
            .build();
    }
}
