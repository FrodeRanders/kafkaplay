package se.fk.kafka.tools;

import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.dataformat.yaml.YAMLFactory;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroupString;
import org.camunda.bpm.model.bpmn.Bpmn;
import org.camunda.bpm.model.bpmn.BpmnModelInstance;
import org.camunda.bpm.model.bpmn.instance.ConditionExpression;
import org.camunda.bpm.model.bpmn.instance.EndEvent;
import org.camunda.bpm.model.bpmn.instance.ExclusiveGateway;
import org.camunda.bpm.model.bpmn.instance.FlowNode;
import org.camunda.bpm.model.bpmn.instance.ParallelGateway;
import org.camunda.bpm.model.bpmn.instance.Process;
import org.camunda.bpm.model.bpmn.instance.SequenceFlow;
import org.camunda.bpm.model.bpmn.instance.StartEvent;
import org.camunda.bpm.model.bpmn.instance.Task;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

public class BpmnScaffolder {
    public static void main(String[] args) {
        ParsedArgs parsed = parseArgs(args);
        if (parsed == null) {
            System.exit(1);
        }

        File bpmnFile = new File(parsed.bpmnPath);
        if (!bpmnFile.exists()) {
            System.err.println("BPMN file not found: " + bpmnFile.getAbsolutePath());
            System.exit(1);
        }

        Path outputRoot = Path.of(parsed.outputDir);
        Path javaOutput = outputRoot.resolve("src/main/java/se/fk/kafka/generated");
        Path coreJavaOutput = outputRoot.resolve("src/main/java/se/fk/kafka");
        Path mainSourceRoot = Path.of("src/main/java");

        BpmnModelInstance model = Bpmn.readModelFromFile(bpmnFile);
        Process process = model.getModelElementsByType(Process.class).stream()
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No <process> element found."));

        String processId = normalize(process.getId() != null ? process.getId() : "process");
        Map<String, NodeInfo> nodes = new LinkedHashMap<>();
        Map<String, List<FlowInfo>> flowsBySource = new LinkedHashMap<>();
        List<String> tasks = new ArrayList<>();

        for (Task task : model.getModelElementsByType(Task.class)) {
            String name = normalize(nameOrId(task.getName(), task.getId()));
            tasks.add(name);
            nodes.put(task.getId(), new NodeInfo(task.getId(), name, NodeType.TASK));
        }

        List<NodeInfo> xors = new ArrayList<>();
        for (ExclusiveGateway gateway : model.getModelElementsByType(ExclusiveGateway.class)) {
            NodeInfo info = new NodeInfo(gateway.getId(), normalize(nameOrId(gateway.getName(), gateway.getId())),
                    NodeType.XOR);
            if (gateway.getDefault() != null) {
                info.defaultFlowId = gateway.getDefault().getId();
            }
            nodes.put(gateway.getId(), info);
            xors.add(info);
        }

        List<NodeInfo> ands = new ArrayList<>();
        for (ParallelGateway gateway : model.getModelElementsByType(ParallelGateway.class)) {
            NodeInfo info = new NodeInfo(gateway.getId(), normalize(nameOrId(gateway.getName(), gateway.getId())),
                    NodeType.AND);
            nodes.put(gateway.getId(), info);
            ands.add(info);
        }

        for (StartEvent startEvent : model.getModelElementsByType(StartEvent.class)) {
            nodes.put(startEvent.getId(), new NodeInfo(startEvent.getId(), "start", NodeType.START));
        }

        for (EndEvent endEvent : model.getModelElementsByType(EndEvent.class)) {
            nodes.put(endEvent.getId(), new NodeInfo(endEvent.getId(), "end", NodeType.END));
        }

        for (SequenceFlow flow : model.getModelElementsByType(SequenceFlow.class)) {
            FlowNode source = flow.getSource();
            FlowNode target = flow.getTarget();
            NodeInfo sourceInfo = nodes.get(source.getId());
            NodeInfo targetInfo = nodes.get(target.getId());
            if (sourceInfo == null || targetInfo == null) {
                continue;
            }
            sourceInfo.outgoingIds.add(targetInfo.id);
            targetInfo.incomingIds.add(sourceInfo.id);
            flowsBySource.computeIfAbsent(sourceInfo.id, key -> new ArrayList<>())
                    .add(flowInfo(flow));
        }

        STGroupString group = new STGroupString(loadTemplates());
        if (!parsed.dryRun) {
            try {
                Files.createDirectories(javaOutput);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to create output directories", e);
            }
        }

        Set<String> existingSources = indexSourceFiles(mainSourceRoot);
        List<String> generatedFiles = new ArrayList<>();

        generateCoreClasses(group, coreJavaOutput, outputRoot, generatedFiles, parsed.dryRun);

        for (String task : tasks) {
            String className = toClassName(task) + (parsed.transactions ? "TransactionalWorker" : "WorkerService");
            Path outputFile = javaOutput.resolve(className + ".java");
            if (Files.exists(outputFile) || existingSources.contains(className + ".java")) {
                continue;
            }
            ST worker = group.getInstanceOf(parsed.transactions ? "transactionalWorkerClass" : "workerClass");
            worker.add("packageName", "se.fk.kafka.generated");
            worker.add("className", className);
            worker.add("processId", processId);
            worker.add("taskId", task);
            if (!parsed.dryRun) {
                writeFile(outputFile, worker.render());
            }
            generatedFiles.add(outputRoot.relativize(outputFile).toString());
        }

        generateStarter(group, javaOutput, outputRoot, generatedFiles, parsed.dryRun, processId);

        generateGateways(processId, group, javaOutput, nodes, flowsBySource, xors, ands, existingSources,
                outputRoot, generatedFiles, parsed.dryRun);
        generateOrchestrator(processId, group, javaOutput, nodes, existingSources, outputRoot, generatedFiles,
                parsed.dryRun);

        String yamlPreview = renderYamlPreview(group, processId, tasks);
        String topicsPreview = renderTopicsPreview(group, processId, tasks);
        String pomPreview = renderPomPreview(group, processId);
        String runLocalPreview = renderRunLocalPreview(group, processId);
        if (!parsed.dryRun) {
            mergeApplicationYaml(processId, tasks, outputRoot);
            Path topicsPath = outputRoot.resolve("topics.sh");
            writeFile(topicsPath, topicsPreview);
            generatedFiles.add(outputRoot.relativize(topicsPath).toString());

            Path pomPath = outputRoot.resolve("pom.xml");
            writeFile(pomPath, pomPreview);
            generatedFiles.add(outputRoot.relativize(pomPath).toString());

            Path runLocalPath = outputRoot.resolve("run-local.sh");
            writeFile(runLocalPath, runLocalPreview);
            generatedFiles.add(outputRoot.relativize(runLocalPath).toString());
        }

        Map<String, Object> summary = buildSummary(processId, tasks, xors, ands, generatedFiles);
        String summaryJson = renderSummaryJson(summary);

        if (!parsed.dryRun) {
            writeSummary(outputRoot, summaryJson);
            writeGeneratedReadme(outputRoot, processId, tasks, xors, ands, generatedFiles);
        }

        System.out.println("Generated in " + outputRoot.toAbsolutePath());
        System.out.println("Process: " + processId);
        System.out.println("Tasks: " + tasks);
        System.out.println("XOR gateways: " + xors.stream().map(info -> info.name).toList());
        System.out.println("AND gateways: " + ands.stream().map(info -> info.name).toList());

        if (parsed.dryRun) {
            System.out.println();
            System.out.println("Dry run preview: summary.json");
            System.out.println(summaryJson);
            System.out.println();
            System.out.println("Dry run preview: application.yml additions");
            System.out.println(yamlPreview);
            System.out.println();
            System.out.println("Dry run preview: topics.sh");
            System.out.println(topicsPreview);
            System.out.println();
            System.out.println("Dry run preview: pom.xml");
            System.out.println(pomPreview);
            System.out.println();
            System.out.println("Dry run preview: run-local.sh");
            System.out.println(runLocalPreview);
        }
    }

    private static ParsedArgs parseArgs(String[] args) {
        if (args.length == 0) {
            System.err.println("Usage: BpmnScaffolder <path-to-bpmn.xml> [output-dir] [--out <dir>] [--dry-run] [--transactions]");
            return null;
        }
        boolean dryRun = false;
        boolean transactions = false;
        String outputDir = null;
        List<String> positional = new ArrayList<>();

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if ("--dry-run".equals(arg)) {
                dryRun = true;
            } else if ("--transactions".equals(arg)) {
                transactions = true;
            } else if (arg.startsWith("--out=")) {
                outputDir = arg.substring("--out=".length());
            } else if ("--out".equals(arg)) {
                if (i + 1 >= args.length) {
                    System.err.println("Missing value for --out");
                    return null;
                }
                outputDir = args[++i];
            } else {
                positional.add(arg);
            }
        }

        if (positional.isEmpty()) {
            System.err.println("Usage: BpmnScaffolder <path-to-bpmn.xml> [output-dir] [--out <dir>] [--dry-run] [--transactions]");
            return null;
        }

        String bpmnPath = positional.get(0);
        if (outputDir == null) {
            outputDir = positional.size() > 1 ? positional.get(1) : "generated";
        }
        return new ParsedArgs(bpmnPath, outputDir, dryRun, transactions);
    }

    private static String normalize(String value) {
        if (value == null || value.isBlank()) {
            return "unnamed";
        }
        String normalized = value.toLowerCase(Locale.ROOT)
                .replaceAll("[^a-z0-9]+", "_")
                .replaceAll("^_+", "")
                .replaceAll("_+$", "");
        return normalized.isBlank() ? "unnamed" : normalized;
    }

    private static String nameOrId(String name, String id) {
        if (name != null && !name.isBlank()) {
            return name;
        }
        if (id != null && !id.isBlank()) {
            return id;
        }
        return "unnamed";
    }

    private static void generateGateways(
            String processId,
            STGroupString group,
            Path javaOutput,
            Map<String, NodeInfo> nodes,
            Map<String, List<FlowInfo>> flowsBySource,
            List<NodeInfo> xors,
            List<NodeInfo> ands,
            Set<String> existingSources,
            Path outputRoot,
            List<String> generatedFiles,
            boolean dryRun
    ) {
        for (NodeInfo xor : xors) {
            if (xor.outgoingIds.size() < 2 || xor.incomingIds.size() != 1) {
                continue;
            }
            Optional<String> inputChannel = resolveInputChannel(processId, nodes, xor.incomingIds.getFirst());
            List<OutputSpec> outputs = resolveOutputChannels(processId, nodes, xor.outgoingIds,
                    flowsBySource.get(xor.id), xor.defaultFlowId);
            if (inputChannel.isEmpty() || outputs.size() < 2) {
                continue;
            }
            outputs = withConditionBlocks(outputs);

            String className = "Xor" + toClassName(xor.name) + "Service";
            Path outputFile = javaOutput.resolve(className + ".java");
            if (Files.exists(outputFile) || existingSources.contains(className + ".java")) {
                continue;
            }
            ST tmpl = group.getInstanceOf("xorGatewayClass");
            tmpl.add("packageName", "se.fk.kafka.generated");
            tmpl.add("className", className);
            tmpl.add("inputChannel", inputChannel.get());
            tmpl.add("outputs", outputs);
            if (!dryRun) {
                writeFile(outputFile, tmpl.render());
            }
            generatedFiles.add(outputRoot.relativize(outputFile).toString());
        }

        for (NodeInfo and : ands) {
            if (and.outgoingIds.size() > 1 && and.incomingIds.size() == 1) {
                Optional<String> inputChannel = resolveInputChannel(processId, nodes, and.incomingIds.getFirst());
                List<OutputSpec> outputs = resolveOutputChannels(processId, nodes, and.outgoingIds, null, null);
                if (inputChannel.isEmpty() || outputs.isEmpty()) {
                    continue;
                }
                String className = "And" + toClassName(and.name) + "SplitService";
                Path outputFile = javaOutput.resolve(className + ".java");
                if (Files.exists(outputFile) || existingSources.contains(className + ".java")) {
                    continue;
                }
                ST tmpl = group.getInstanceOf("andSplitGatewayClass");
                tmpl.add("packageName", "se.fk.kafka.generated");
                tmpl.add("className", className);
                tmpl.add("inputChannel", inputChannel.get());
                tmpl.add("outputs", outputs);
                if (!dryRun) {
                    writeFile(outputFile, tmpl.render());
                }
                generatedFiles.add(outputRoot.relativize(outputFile).toString());
            } else if (and.incomingIds.size() > 1) {
                List<JoinMethodSpec> incomingMethods = resolveIncomingMethods(processId, nodes, and.incomingIds);
                Optional<OutputSpec> output = resolveSingleOutput(processId, nodes, and.outgoingIds);
                if (incomingMethods.isEmpty() || output.isEmpty()) {
                    continue;
                }
                String className = "And" + toClassName(and.name) + "JoinService";
                Path outputFile = javaOutput.resolve(className + ".java");
                if (Files.exists(outputFile) || existingSources.contains(className + ".java")) {
                    continue;
                }
                ST tmpl = group.getInstanceOf("andJoinGatewayClass");
                tmpl.add("packageName", "se.fk.kafka.generated");
                tmpl.add("className", className);
                tmpl.add("processId", processId);
                tmpl.add("outputChannel", output.get().channel);
                tmpl.add("outputTaskId", output.get().taskId);
                tmpl.add("incomingMethodsBlock", buildJoinMethodsBlock(incomingMethods));
                tmpl.add("joinCondition", buildJoinCondition(incomingMethods));
                if (!dryRun) {
                    writeFile(outputFile, tmpl.render());
                }
                generatedFiles.add(outputRoot.relativize(outputFile).toString());
            }
        }
    }

    private static void generateOrchestrator(
            String processId,
            STGroupString group,
            Path javaOutput,
            Map<String, NodeInfo> nodes,
            Set<String> existingSources,
            Path outputRoot,
            List<String> generatedFiles,
            boolean dryRun
    ) {
        List<JoinMethodSpec> incomingMethods = new ArrayList<>();
        for (NodeInfo node : nodes.values()) {
            if (node.type != NodeType.TASK) {
                continue;
            }
            if (!node.outgoingIds.isEmpty()) {
                continue;
            }
            String channel = processId + "_" + node.name + "_output";
            incomingMethods.add(new JoinMethodSpec("on" + toClassName(node.name) + "Completed", channel, node.name));
        }
        if (incomingMethods.isEmpty()) {
            return;
        }
        String className = "ProcessOrchestratorService";
        Path outputFile = javaOutput.resolve(className + ".java");
        if (Files.exists(outputFile) || existingSources.contains(className + ".java")) {
            return;
        }
        ST tmpl = group.getInstanceOf("orchestratorClass");
        tmpl.add("packageName", "se.fk.kafka.generated");
        tmpl.add("className", className);
        tmpl.add("processId", processId);
        tmpl.add("incomingMethodsBlock", buildOrchestratorMethodsBlock(incomingMethods));
        if (!dryRun) {
            writeFile(outputFile, tmpl.render());
        }
        generatedFiles.add(outputRoot.relativize(outputFile).toString());
    }

    private static Optional<String> resolveInputChannel(
            String processId,
            Map<String, NodeInfo> nodes,
            String sourceId
    ) {
        NodeInfo source = nodes.get(sourceId);
        if (source == null) {
            return Optional.empty();
        }
        if (source.type == NodeType.TASK) {
            return Optional.of(processId + "_" + source.name + "_output");
        }
        if (source.type == NodeType.START) {
            return Optional.of(processId + "_start");
        }
        return Optional.empty();
    }

    private static List<OutputSpec> resolveOutputChannels(
            String processId,
            Map<String, NodeInfo> nodes,
            List<String> targetIds,
            List<FlowInfo> flows,
            String defaultFlowId
    ) {
        List<OutputSpec> outputs = new ArrayList<>();
        for (String targetId : targetIds) {
            NodeInfo target = nodes.get(targetId);
            if (target == null || target.type != NodeType.TASK) {
                continue;
            }
            FlowInfo flowInfo = findFlowInfo(flows, target.id);
            String condition = flowInfo != null ? flowInfo.condition : null;
            boolean isDefault = flowInfo != null && flowInfo.id.equals(defaultFlowId);
            outputs.add(new OutputSpec(
                    "task" + toClassName(target.name) + "Emitter",
                    processId + "_" + target.name + "_input",
                    target.name,
                    condition,
                    isDefault,
                    null
            ));
        }
        return outputs;
    }

    private static Optional<OutputSpec> resolveSingleOutput(
            String processId,
            Map<String, NodeInfo> nodes,
            List<String> targetIds
    ) {
        List<OutputSpec> outputs = resolveOutputChannels(processId, nodes, targetIds, null, null);
        if (outputs.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(outputs.getFirst());
    }

    private static List<JoinMethodSpec> resolveIncomingMethods(
            String processId,
            Map<String, NodeInfo> nodes,
            List<String> sourceIds
    ) {
        List<JoinMethodSpec> methods = new ArrayList<>();
        for (String sourceId : sourceIds) {
            NodeInfo source = nodes.get(sourceId);
            if (source == null || source.type != NodeType.TASK) {
                continue;
            }
            String channel = processId + "_" + source.name + "_output";
            methods.add(new JoinMethodSpec("on" + toClassName(source.name), channel, source.name));
        }
        return methods;
    }

    private static String renderYamlPreview(STGroupString group, String processId, List<String> tasks) {
        ST yaml = group.getInstanceOf("applicationYaml");
        yaml.add("processId", processId);
        yaml.add("tasks", tasks);
        return yaml.render();
    }

    private static String renderTopicsPreview(STGroupString group, String processId, List<String> tasks) {
        ST topicsScript = group.getInstanceOf("topicsScript");
        topicsScript.add("processId", processId);
        topicsScript.add("tasks", tasks);
        return topicsScript.render();
    }

    private static String renderPomPreview(STGroupString group, String processId) {
        ST pom = group.getInstanceOf("pomXml");
        pom.add("processId", processId);
        pom.add("starterClass", toClassName(processId));
        return pom.render();
    }

    private static String renderRunLocalPreview(STGroupString group, String processId) {
        ST script = group.getInstanceOf("runLocalScript");
        script.add("processId", processId);
        script.add("starterClass", toClassName(processId));
        return script.render();
    }

    private static void generateCoreClasses(
            STGroupString group,
            Path coreJavaOutput,
            Path outputRoot,
            List<String> generatedFiles,
            boolean dryRun
    ) {
        writeCoreClass(group, coreJavaOutput, outputRoot, generatedFiles, dryRun,
                "ProcessEvent.java", "processEventClass");
        writeCoreClass(group, coreJavaOutput, outputRoot, generatedFiles, dryRun,
                "ProcessState.java", "processStateClass");
        writeCoreClass(group, coreJavaOutput, outputRoot, generatedFiles, dryRun,
                "ProcessStateStore.java", "processStateStoreClass");
    }

    private static void writeCoreClass(
            STGroupString group,
            Path coreJavaOutput,
            Path outputRoot,
            List<String> generatedFiles,
            boolean dryRun,
            String fileName,
            String templateName
    ) {
        Path outputFile = coreJavaOutput.resolve(fileName);
        if (Files.exists(outputFile)) {
            return;
        }
        ST template = group.getInstanceOf(templateName);
        if (!dryRun) {
            writeFile(outputFile, template.render());
        }
        generatedFiles.add(outputRoot.relativize(outputFile).toString());
    }

    private static void generateStarter(
            STGroupString group,
            Path javaOutput,
            Path outputRoot,
            List<String> generatedFiles,
            boolean dryRun,
            String processId
    ) {
        String className = toClassName(processId) + "Starter";
        Path outputFile = javaOutput.resolve(className + ".java");
        if (Files.exists(outputFile)) {
            return;
        }
        ST template = group.getInstanceOf("starterClass");
        template.add("packageName", "se.fk.kafka.generated");
        template.add("className", className);
        template.add("processId", processId);
        if (!dryRun) {
            writeFile(outputFile, template.render());
        }
        generatedFiles.add(outputRoot.relativize(outputFile).toString());
    }

    private static void mergeApplicationYaml(String processId, List<String> tasks, Path outputRoot) {
        Path path = Path.of("src/main/resources/application.yml");
        Path outputPath = path;
        if (!Files.exists(path)) {
            outputPath = outputRoot.resolve("src/main/resources/application.yml");
        }
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        Map<String, Object> root = new LinkedHashMap<>();
        if (Files.exists(outputPath)) {
            root = mapper.readValue(outputPath.toFile(), new TypeReference<>() {
            });
        }

        Map<String, Object> mp = ensureMap(root, "mp");
        Map<String, Object> messaging = ensureMap(mp, "messaging");
        Map<String, Object> incoming = ensureMap(messaging, "incoming");
        Map<String, Object> outgoing = ensureMap(messaging, "outgoing");

        for (String task : tasks) {
            String incomingKey = processId + "_" + task + "_input";
            incoming.putIfAbsent(incomingKey, channelConfig(
                    "smallrye-kafka",
                    incomingKey,
                    "org.apache.kafka.common.serialization.StringDeserializer"
            ));

            String outgoingKey = processId + "_" + task + "_output";
            outgoing.putIfAbsent(outgoingKey, channelConfig(
                    "smallrye-kafka",
                    outgoingKey,
                    "org.apache.kafka.common.serialization.StringSerializer"
            ));
        }

        try {
            Files.createDirectories(outputPath.getParent());
            String yaml = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(root);
            Files.writeString(outputPath, yaml, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to write application.yml", e);
        }
    }

    private static Map<String, Object> channelConfig(String connector, String topic, String serializerOrDeserializer) {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("connector", connector);
        config.put("topic", topic);
        Map<String, Object> value = new LinkedHashMap<>();
        if (serializerOrDeserializer.contains("Deserializer")) {
            value.put("deserializer", serializerOrDeserializer);
        } else {
            value.put("serializer", serializerOrDeserializer);
        }
        config.put("value", value);
        return config;
    }

    private static Map<String, Object> ensureMap(Map<String, Object> root, String key) {
        Object value = root.get(key);
        if (value instanceof Map<?, ?> mapValue) {
            @SuppressWarnings("unchecked")
            Map<String, Object> casted = (Map<String, Object>) mapValue;
            return casted;
        }
        Map<String, Object> created = new LinkedHashMap<>();
        root.put(key, created);
        return created;
    }

    private static class NodeInfo {
        private final String id;
        private final String name;
        private final NodeType type;
        private final List<String> incomingIds = new ArrayList<>();
        private final List<String> outgoingIds = new ArrayList<>();
        private String defaultFlowId;

        private NodeInfo(String id, String name, NodeType type) {
            this.id = id;
            this.name = name;
            this.type = type;
        }
    }

    private enum NodeType {
        TASK,
        XOR,
        AND,
        START,
        END
    }

    public static class OutputSpec {
        public final String emitter;
        public final String channel;
        public final String taskId;
        public final String condition;
        public final boolean isDefault;
        public final String conditionBlock;

        public OutputSpec(
                String emitter,
                String channel,
                String taskId,
                String condition,
                boolean isDefault,
                String conditionBlock
        ) {
            this.emitter = emitter;
            this.channel = channel;
            this.taskId = taskId;
            this.condition = condition;
            this.isDefault = isDefault;
            this.conditionBlock = conditionBlock;
        }
    }

    private record JoinMethodSpec(String method, String channel, String activityId) {
    }

    private record FlowInfo(String id, String targetId, String condition) {
    }

    private record ParsedArgs(String bpmnPath, String outputDir, boolean dryRun, boolean transactions) {
    }

    private static String toClassName(String value) {
        String[] parts = normalize(value).split("_");
        StringBuilder builder = new StringBuilder();
        for (String part : parts) {
            if (part.isBlank()) {
                continue;
            }
            builder.append(part.substring(0, 1).toUpperCase(Locale.ROOT))
                    .append(part.substring(1));
        }
        return builder.length() == 0 ? "Unnamed" : builder.toString();
    }

    private static String loadTemplates() {
        try (InputStream input = BpmnScaffolder.class.getResourceAsStream("/templates/scaffold.stg")) {
            if (input == null) {
                throw new IllegalStateException("Template not found: /templates/scaffold.stg");
            }
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load templates", e);
        }
    }

    private static void writeFile(Path path, String content) {
        try {
            Files.createDirectories(path.getParent());
            Files.writeString(path, content, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to write " + path, e);
        }
    }

    private static FlowInfo flowInfo(SequenceFlow flow) {
        ConditionExpression expression = flow.getConditionExpression();
        String condition = expression != null ? expression.getTextContent() : null;
        String normalized = condition != null ? condition.trim() : null;
        return new FlowInfo(flow.getId(), flow.getTarget().getId(), normalized);
    }

    private static FlowInfo findFlowInfo(List<FlowInfo> flows, String targetId) {
        if (flows == null) {
            return null;
        }
        for (FlowInfo flow : flows) {
            if (flow.targetId.equals(targetId)) {
                return flow;
            }
        }
        return null;
    }

    private static Set<String> indexSourceFiles(Path root) {
        Set<String> files = new TreeSet<>();
        if (!Files.exists(root)) {
            return files;
        }
        try {
            Files.walk(root)
                    .filter(path -> path.getFileName().toString().endsWith(".java"))
                    .forEach(path -> files.add(path.getFileName().toString()));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to index source files", e);
        }
        return files;
    }

    private static List<OutputSpec> withConditionBlocks(List<OutputSpec> outputs) {
        List<OutputSpec> updated = new ArrayList<>();
        boolean hasDefault = outputs.stream().anyMatch(output -> output.isDefault);
        boolean hasCondition = outputs.stream().anyMatch(output -> output.condition != null);
        for (int i = 0; i < outputs.size(); i++) {
            OutputSpec output = outputs.get(i);
            String conditionExpr;
            if (output.condition != null) {
                conditionExpr = "evaluateCondition(\"" + escapeJava(output.condition) + "\", inputEvent)";
            } else if (output.isDefault) {
                conditionExpr = "true";
            } else if (!hasCondition && !hasDefault && i == 0) {
                conditionExpr = "true";
            } else {
                continue;
            }
            String block = "        if (" + conditionExpr + ") {\n"
                    + "            " + output.emitter + ".send(msg.getPayload());\n"
                    + "            return msg.ack();\n"
                    + "        }\n";
            updated.add(new OutputSpec(output.emitter, output.channel, output.taskId, output.condition,
                    output.isDefault, block));
        }
        return updated;
    }

    private static String buildJoinCondition(List<JoinMethodSpec> incomingMethods) {
        if (incomingMethods.isEmpty()) {
            return "false";
        }
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < incomingMethods.size(); i++) {
            if (i > 0) {
                builder.append(" && ");
            }
            JoinMethodSpec method = incomingMethods.get(i);
            builder.append("tokens.stream().anyMatch(token -> \"")
                    .append(method.activityId)
                    .append("\".equals(token.at()))");
        }
        return builder.toString();
    }

    private static String buildJoinMethodsBlock(List<JoinMethodSpec> incomingMethods) {
        StringBuilder builder = new StringBuilder();
        for (JoinMethodSpec method : incomingMethods) {
            builder.append("    @Incoming(\"")
                    .append(method.channel)
                    .append("\")\n")
                    .append("    public CompletionStage<Void> ")
                    .append(method.method)
                    .append("(Message<String> msg) {\n")
                    .append("        return handleJoin(\"")
                    .append(method.activityId)
                    .append("\", msg);\n")
                    .append("    }\n\n");
        }
        return builder.toString();
    }

    private static String buildOrchestratorMethodsBlock(List<JoinMethodSpec> incomingMethods) {
        StringBuilder builder = new StringBuilder();
        for (JoinMethodSpec method : incomingMethods) {
            builder.append("    @Incoming(\"")
                    .append(method.channel)
                    .append("\")\n")
                    .append("    public CompletionStage<Void> ")
                    .append(method.method)
                    .append("(Message<String> msg) {\n")
                    .append("        return handleCompletion(msg);\n")
                    .append("    }\n\n");
        }
        return builder.toString();
    }

    private static String escapeJava(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static Map<String, Object> buildSummary(
            String processId,
            List<String> tasks,
            List<NodeInfo> xors,
            List<NodeInfo> ands,
            List<String> generatedFiles
    ) {
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("processId", processId);
        summary.put("tasks", tasks);
        summary.put("xorGateways", xors.stream().map(info -> info.name).toList());
        summary.put("andGateways", ands.stream().map(info -> info.name).toList());
        summary.put("generatedFiles", generatedFiles);
        return summary;
    }

    private static String renderSummaryJson(Map<String, Object> summary) {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(summary);
    }

    private static void writeSummary(Path outputRoot, String summaryJson) {
        Path summaryPath = outputRoot.resolve("summary.json");
        try {
            Files.createDirectories(summaryPath.getParent());
            Files.writeString(summaryPath, summaryJson, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to write summary.json", e);
        }
    }

    private static void writeGeneratedReadme(
            Path outputRoot,
            String processId,
            List<String> tasks,
            List<NodeInfo> xors,
            List<NodeInfo> ands,
            List<String> generatedFiles
    ) {
        Path readmePath = outputRoot.resolve("README.md");
        StringBuilder builder = new StringBuilder();
        builder.append("# Generated BPMN Scaffolding\n\n");
        builder.append("Process: `").append(processId).append("`\n\n");
        builder.append("## Tasks\n");
        for (String task : tasks) {
            builder.append("- ").append(task).append("\n");
        }
        builder.append("\n## Gateways\n");
        builder.append("- XOR: ").append(xors.stream().map(info -> info.name).toList()).append("\n");
        builder.append("- AND: ").append(ands.stream().map(info -> info.name).toList()).append("\n");
        builder.append("\n## Generated Files\n");
        for (String file : generatedFiles) {
            builder.append("- ").append(file).append("\n");
        }
        builder.append("\n## Smoke Test Starter\n");
        builder.append("Start a process instance with:\n\n");
        builder.append("```\n");
        builder.append("\n## Run Local Script\n");
        builder.append("Use `run-local.sh` to build and run the starter. Set `START_KAFKA=true` to launch Kafka.\n");
        builder.append("mvn -q clean package\n");
        builder.append("java -cp target/")
                .append(processId)
                .append("-generated-1.0-SNAPSHOT-all.jar se.fk.kafka.generated.")
                .append(toClassName(processId))
                .append("Starter localhost:9094 valid=true\n");
        builder.append("```\n");
        builder.append("\n## Notes\n");
        builder.append("- The generator skips classes that already exist in `src/main/java/`.\n");
        builder.append("- XOR conditions are generated as placeholders; implement your expression logic.\n");
        builder.append("- `application.yml` was merged with new channels; formatting/comments may change.\n");
        try {
            Files.createDirectories(readmePath.getParent());
            Files.writeString(readmePath, builder.toString(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to write generated README", e);
        }
    }
}
