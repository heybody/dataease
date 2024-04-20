package io.dataease.utils;

import io.dataease.constant.AuthConstant;
import io.dataease.exception.DEException;
import org.apache.commons.lang3.StringUtils;
import org.springframework.core.env.Environment;

import java.util.List;
import java.util.Objects;

public class WhitelistUtils {

    private static String contextPath;


    public static String getContextPath() {
        if (StringUtils.isBlank(contextPath)) {
            contextPath = Objects.requireNonNull(CommonBeanFactory.getBean(Environment.class)).getProperty("server.servlet.context-path", String.class);
        }
        return contextPath;
    }

    public static List<String> WHITE_PATH = List.of(
            "/login/localLogin",
            "/apisix/check",
            "/dekey",
            "/index.html",
            "/model",
            "/swagger-resources",
            "/doc.html",
            "/panel.html",
            "/mobile.html",
            "/lark/info",
            "/lark/token",
            "/larksuite/info",
            "/larksuite/token",
            "/dingtalk/info",
            "/dingtalk/token",
            "/wecom/info",
            "/wecom/token",
            "/sysParameter/requestTimeOut",
            "/setting/authentication/status",
            "/sysParameter/ui",
            "/embedded/initIframe",
            "/");

    public static boolean match(String requestURI) {
        if (requestURI.contains(";") && !requestURI.contains("?")) {
            DEException.throwException("Invalid uri: " + requestURI);
        }
        if (StringUtils.startsWith(requestURI, getContextPath())) {
            requestURI = requestURI.replaceFirst(getContextPath(), "");
        }
        if (StringUtils.startsWith(requestURI, AuthConstant.DE_API_PREFIX)) {
            requestURI = requestURI.replaceFirst(AuthConstant.DE_API_PREFIX, "");
        }
        return WHITE_PATH.contains(requestURI)
                || StringUtils.endsWithAny(requestURI, ".ico", "js", ".css", "svg", "png", "jpg", "js.map", ".otf", ".ttf", ".woff2")
                || StringUtils.startsWithAny(requestURI, "data:image")
                || StringUtils.startsWithAny(requestURI, "/login/platformLogin/")
                || StringUtils.startsWithAny(requestURI, "/static-resource/")
                || StringUtils.startsWithAny(requestURI, "/appearance/image/")
                || StringUtils.startsWithAny(requestURI, "/share/proxyInfo")
                || StringUtils.startsWithAny(requestURI, "/xpackComponent/content/")
                || StringUtils.startsWithAny(requestURI, "/geo/")
                || StringUtils.startsWithAny(requestURI, "/map/" )
                || StringUtils.startsWithAny(requestURI, "/share/" )
                || StringUtils.startsWithAny(requestURI, "/doc.html" )
                || StringUtils.startsWithAny(requestURI, "/swagger-ui" )
                || StringUtils.startsWithAny(requestURI, "/v3/api-docs/");
    }
}
