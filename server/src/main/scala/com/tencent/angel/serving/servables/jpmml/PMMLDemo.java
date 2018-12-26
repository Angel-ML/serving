package com.tencent.angel.serving.servables.jpmml;

import org.dmg.pmml.FieldName;
import org.dmg.pmml.PMML;
import org.jpmml.evaluator.*;
import org.jpmml.model.PMMLUtil;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PMMLDemo {
    private PMML pmml = new PMML();
    private Evaluator evaluator = null;

    public PMMLDemo (String pmmlModelFileName) {
        InputStream inputStream = null;
        try {
            inputStream = new FileInputStream(pmmlModelFileName);
            pmml = PMMLUtil.unmarshal(inputStream);
        } catch (IOException | JAXBException | SAXException e1) {
            e1.printStackTrace();
        } finally {
            //关闭输入流
            try {
                if (inputStream != null) {
                    inputStream.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        ModelEvaluatorFactory modelEvaluatorFactory = ModelEvaluatorFactory.newInstance();
        evaluator = modelEvaluatorFactory.newModelEvaluator(pmml);
    }

    public int predict(double a, double b, double c, double d) {
        Map<String, Double> data = new HashMap<String, Double>();
        data.put("x1", a);
        data.put("x2", b);
        data.put("x3", c);
        data.put("x4", d);

        List<InputField> inputFields = evaluator.getInputFields();

        //过模型的原始特征，从画像中获取数据，作为模型输入
        Map<FieldName, FieldValue> arguments = new LinkedHashMap<FieldName, FieldValue>();
        for (InputField inputField : inputFields) {
            FieldName inputFieldName = inputField.getName();
            Object rawValue = data.get(inputFieldName.getValue());
            FieldValue inputFieldValue = inputField.prepare(rawValue);
            arguments.put(inputFieldName, inputFieldValue);
        }

        Map<FieldName, ?> results = evaluator.evaluate(arguments);
        List<TargetField> targetFields = evaluator.getTargetFields();

        TargetField targetField = targetFields.get(0);
        FieldName targetFieldName = targetField.getName();

        Object targetFieldValue = results.get(targetFieldName);
        System.out.println("target: " + targetFieldName.getValue() + " value: " + targetFieldValue);
        int primitiveValue = -1;
        if (targetFieldValue instanceof Computable) {
            Computable computable = (Computable) targetFieldValue;
            primitiveValue = (Integer) computable.getResult();
        }
        System.out.println(a + " " + b + " " + c + " " + d + ":" + primitiveValue);
        return primitiveValue;
    }

    public static void main(String args[]) {
        PMMLDemo demo = new PMMLDemo("E:\\github\\fitzwang\\serving\\pycode\\LogisticRegressionIris.pmml");
        demo.predict(6.2,2.2,4.5,1.5);
        demo.predict(6.1,3,4.6,1.4);
        demo.predict(5.7,3.8,1.7,0.3);
    }
}