CREATE  OR REPLACE
    ALGORITHM = UNDEFINED 
    DEFINER = `root`@`localhost` 
    SQL SECURITY DEFINER
VIEW `wzhyup` AS
    SELECT 
        '01' AS `rtype`,
        `a`.`NBXH` AS `uuid`,
        ifnull(nullif(`a`.`USCC`,''),'--') AS `社会信用代码`,
        (CASE
            WHEN
                ((`a`.`INFOACTIONTYPE` = '2')
                    OR (`b`.`NBXH` IS NOT NULL))
            THEN
                '6'
            ELSE '2'
        END) AS `工商业务类型`,
        SUBSTR(`a`.`USCC`, 10, 9) AS `组织机构代码`,
        ifnull(nullif(`a`.`ZCH`,''),'--') AS `注册号`,
        `a`.`QYMC` AS `详细名称`,
        `a`.`QYLX` AS `企业类型`,
        `c`.`单位类型` AS `单位类型`,
        `c`.`登记注册类型` AS `登记注册类型`,
        `c`.`控股情况` AS `控股情况`,
        `c`.`机构类型` AS `机构类型`,
        `c`.`执行会计标准类别` AS `执行会计标准类别`,
        CAST(NULL AS CHAR (10) CHARSET UTF8) AS `企业登记机关`,
        IF((`a`.`DJJG` = '330000'),
            IFNULL(`a`.`ZSDJJG`, `a`.`DJJG`),
            `a`.`DJJG`) AS `数据处理地代码`,
        `a`.`CLRQ` AS `开业日期`,
        `a`.`JYQSRQ` AS `经营期限自`,
        `a`.`JYJZRQ` AS `经营期限止`,
        IF(((`a`.`JYQSRQ` IS NOT NULL)
                AND (`a`.`JYJZRQ` IS NOT NULL)),
            CAST(((CAST(SUBSTR(`a`.`JYJZRQ`, 1, 4) AS DECIMAL (10 , 0 )) - CAST(SUBSTR(`a`.`JYQSRQ`, 1, 4) AS DECIMAL (10 , 0 ))) + 1)
                AS CHAR (4) CHARSET UTF8),
            '') AS `经营期限`,
        `a`.`ZS` AS `住所`,
        `a`.`YZBM` AS `邮政编码`,
        `d`.`GDDH` AS `法人固话`,
        `a`.`ProLoc` AS `生产经营地址`,
        `a`.`JYFW` AS `经营范围`,
        CAST(NULL AS CHAR (10) CHARSET UTF8) AS `注册资本`,
        CAST(NULL AS CHAR (10) CHARSET UTF8) AS `货币种类`,
        CAST(NULL AS CHAR (10) CHARSET UTF8) AS `货币金额`,
        IFNULL(`a`.`INFOACTIONTYPE`, '0') AS `信息操作类型`,
        CAST(NULL AS CHAR (100) CHARSET UTF8) AS `数据修改时间`,
        IF((`a`.`DJJG` = '330000'),
            IFNULL(`a`.`ZSDJJG`, `a`.`DJJG`),
            `a`.`DJJG`) AS `行政区划代码`,
        CAST(NULL AS CHAR (10) CHARSET UTF8) AS `数据包编码`,
        (CASE
            WHEN
                ((`a`.`INFOACTIONTYPE` = '2')
                    OR (`b`.`NBXH` IS NOT NULL))
            THEN
                '1'
            ELSE '0'
        END) AS `是否注销`,
        CAST(NULL AS CHAR (10) CHARSET UTF8) AS `批次号`,
        CAST(NULL AS CHAR (10) CHARSET UTF8) AS `上级主管部门名称`,
        `a`.`FDDBR` AS `法定代表人`,
        `e`.`INV` AS `财务负责人`,
        CAST(NULL AS CHAR (10) CHARSET UTF8) AS `投资人数量`,
        CAST(NULL AS CHAR (10) CHARSET UTF8) AS `下级子公司数量`,
        CAST(NULL AS CHAR (10) CHARSET UTF8) AS `变更时间`,
        '999' AS `状态`,
        '0' AS `是否重码`,
        '0' AS `人工处理结果类型`,
        '0' AS `是否已审核`,
        '1' AS `是否已推送到名录库`,
        CAST(NULL AS CHAR (100) CHARSET UTF8) AS `数据上传时间`,
        CAST(NULL AS CHAR (10) CHARSET UTF8) AS `审核时间`
    FROM
        ((((`zz_gsyyzz` `a`
        LEFT JOIN `gx_zxdjxx` `b` ON ((`a`.`NBXH` = `b`.`NBXH`)))
        LEFT JOIN `cdetrs` `c` ON ((`a`.`QYLX` = `c`.`代码`)))
        LEFT JOIN `gs_frxx` `d` ON (((`a`.`NBXH` = `d`.`NBXH`)
            AND (`d`.`RYXH` = '1'))))
        LEFT JOIN `gs_cwryxx` `e` ON ((`a`.`NBXH` = `e`.`NBXH`)))
    WHERE
        (`a`.`NBXH` IS NOT NULL)