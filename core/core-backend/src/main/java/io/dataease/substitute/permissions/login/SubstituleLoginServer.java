package io.dataease.substitute.permissions.login;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTCreator;
import com.auth0.jwt.algorithms.Algorithm;
import io.dataease.api.permissions.login.dto.PwdLoginDTO;
import io.dataease.auth.bo.TokenUserBO;
import io.dataease.auth.vo.TokenVO;
import io.dataease.exception.DEException;
import io.dataease.result.ResultCode;
import io.dataease.utils.LogUtil;
import io.dataease.utils.RsaUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.*;

@Component
@ConditionalOnMissingBean(name = "sc")
@RestController
@RequestMapping
public class SubstituleLoginServer {


    @PostMapping("/login/localLogin")
    public TokenVO localLogin(@RequestBody  PwdLoginDTO dto) {
        TokenUserBO tokenUserBO = new TokenUserBO();
        try {
            String user = RsaUtils.decryptStr(dto.getName());
            String passwd = RsaUtils.decryptStr(dto.getPwd());
            if (StringUtils.equals("admin", user) && StringUtils.equals(passwd, "Eslink@2023!")) {
                tokenUserBO.setUserId(1L);
                tokenUserBO.setDefaultOid(1L);
                String md5Pwd = "83d923c9f1d8fcaa46cae0ed2aaa81b5";
                return generate(tokenUserBO, md5Pwd);
            }
            throw new DEException(ResultCode.USER_LOGIN_ERROR.code(), ResultCode.USER_LOGIN_ERROR.message());
        } catch (Exception ex) {
            throw DEException.getException("鉴权错误");
        }

    }


    @GetMapping("/logout")
    public void logout() {
        LogUtil.info("substitule logout");
    }

    private TokenVO generate(TokenUserBO bo, String secret) {
        Algorithm algorithm = Algorithm.HMAC256(secret);
        Long userId = bo.getUserId();
        Long defaultOid = bo.getDefaultOid();
        JWTCreator.Builder builder = JWT.create();
        builder.withClaim("uid", userId).withClaim("oid", defaultOid);
        String token = builder.sign(algorithm);
        return new TokenVO(token, 0L);
    }
}
