package com.develop.oss.minio.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.develop.oss.minio.common.R;
import com.develop.oss.minio.dto.MinioUploadDto;
import io.minio.MinioClient;
import io.minio.Result;
import io.minio.messages.Bucket;
import io.minio.messages.Item;
import io.minio.policy.PolicyType;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author changchen
 * @create 2020-08-12 11:02
 */
@Api(tags = "MinioController", description = "MinIO对象存储管理")
@RestController
@RequestMapping("/minio")
@Slf4j
public class MinioController {

    @Value("${minio.endpoint}")
    private String ENDPOINT;
    @Value("${minio.bucketName}")
    private String BUCKET_NAME;
    @Value("${minio.accessKey}")
    private String ACCESS_KEY;
    @Value("${minio.secretKey}")
    private String SECRET_KEY;

    @ApiOperation("文件上传")
    @PostMapping("/upload")
    @ResponseBody
    public R upload(@RequestParam("file") MultipartFile file) {
        try {
            //创建一个MinIO的Java客户端
            MinioClient minioClient = new MinioClient(ENDPOINT, ACCESS_KEY, SECRET_KEY);
            //注意有桶名存在 否则isExist==false
            boolean isExist = minioClient.bucketExists(BUCKET_NAME);
            if (isExist) {
                log.info("存储桶已经存在！");
            } else {
                //创建存储桶并设置只读权限
                minioClient.makeBucket(BUCKET_NAME);
                minioClient.setBucketPolicy(BUCKET_NAME, "*.*", PolicyType.READ_ONLY);
            }
            String filename = file.getOriginalFilename();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            // 设置存储对象名称
            String objectName = sdf.format(new Date()) + "/" + filename;
            // 使用putObject上传一个文件到存储桶中
            minioClient.putObject(BUCKET_NAME, objectName, file.getInputStream(), file.getContentType());
            log.info("文件上传成功!");
            MinioUploadDto minioUploadDto = new MinioUploadDto();
            minioUploadDto.setName(filename);
            minioUploadDto.setUrl(ENDPOINT + "/" + BUCKET_NAME + "/" + objectName);
            Map map =(Map)JSONObject.parse(JSON.toJSONString(minioUploadDto));
            return R.ok().data(map);
        } catch (Exception e) {
            log.info("上传发生错误: {}！", e.getMessage());
        }
        return R.error();
    }

    /**
     * 这里的objectName：20200812/f2.jpg
     * 除了桶名
     * @param objectName
     * @return
     */
    @ApiOperation("文件删除")
    @PostMapping("/delete")
    @ResponseBody
    public R delete(@RequestParam("objectName") String objectName) {
        try {
            MinioClient minioClient = new MinioClient(ENDPOINT, ACCESS_KEY, SECRET_KEY);
            minioClient.removeObject(BUCKET_NAME, objectName);
            return R.ok().message("删除成功");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return R.error();
    }

    /**
     * 根据桶名查看该桶下的所有文件
     * 并将查询出来的文件 写入到本地
     * @param bucketName
     * @return
     */
    @ApiOperation("查看当前桶下的所有文件")
    @PostMapping("/list")
    @ResponseBody
    public R list(@RequestParam("bucketName") String bucketName) {
        FileOutputStream fos = null;
        InputStream object = null;
        try {
            MinioClient minioClient = new MinioClient(ENDPOINT, ACCESS_KEY, SECRET_KEY);
            //先判断 bucketName是否存在
            boolean bucketCheck = minioClient.bucketExists(bucketName);
            if (bucketCheck){
                Iterable<Result<Item>> results = minioClient.listObjects(bucketName);
                StringBuffer sbf = null;

                List<MinioUploadDto> dtoList = new ArrayList<>();
                for (Result<Item> result : results) {
                    Item item = result.get();
                    String key = (String)item.get("Key");
                    sbf = new StringBuffer("D:/minio/");
                    //根据桶名 文件名 查看到文件流
                    object = minioClient.getObject(bucketName, key);
                    String[] split = key.split("/");
                    if (split != null && split.length >0){
                        for (String sp : split) {
                            sbf.append(sp);
                            sbf.append("/");
                        }
                        sbf.deleteCharAt(sbf.length()-1);
                    }

                    System.out.println(sbf.toString());
                    File file = new File(sbf.toString());
                    boolean checkExist = file.isFile();
                    if (checkExist){
                        fos = new FileOutputStream(new File(sbf.toString()));
                    }else {
                        //判断 是否需要创建文件夹
                        if (split.length > 1){
                            //截取最后一个/前的数据
                            String path = sbf.toString().substring(0,sbf.toString().lastIndexOf("/")).toString();
                            System.out.println(path);
                            file = new File(path);
                            //创建文件夹
                            file.mkdirs();
                            System.out.println(sbf.toString());
                            //创建文件
                            file.createNewFile();
                        }else {
                            //直接创建文件夹
                            file.createNewFile();
                        }

                        fos = new FileOutputStream(new File(sbf.toString()));

                    }
                    byte[] buffer = new byte[1024];
                    int len;
                    while ( (len = object.read(buffer)) != -1){
                        fos.write(buffer,0,len);
                    }

                    //清空 sbf
                    sbf = new StringBuffer();
                    MinioUploadDto dto = new MinioUploadDto();
                    dto.setUrl(ENDPOINT+"/"+bucketName+"/"+key);
                    dtoList.add(dto);
                }

                return R.ok().data("results",dtoList);
            }else {
                //
                return R.error().message("该桶名不存在，请输入正确的桶名！");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                //关闭流
                if (object != null){
                    object.close();
                }
                if (fos != null){
                    fos.close();
                }
            }catch (IOException e){
                e.getLocalizedMessage();
            }
        }
        return R.error();
    }

    /**
     * 查看所有的桶名
     * @return
     */
    @ApiOperation("查看当前桶下的所有文件")
    @GetMapping("/queryBuckets")
    @ResponseBody
    public R queryBuckets() {
        try {
            MinioClient minioClient = new MinioClient(ENDPOINT, ACCESS_KEY, SECRET_KEY);

            //查看所有桶
            List<String> nameList = new ArrayList<>();
            List<Bucket> buckets = minioClient.listBuckets();
            if (!CollectionUtils.isEmpty(buckets)){
                buckets.forEach(bk ->{
                    nameList.add(bk.name());
                });
            }

            return R.ok().data("list",nameList);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return R.error();
    }


}
