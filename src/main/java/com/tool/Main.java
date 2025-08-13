package com.tool;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) {
        PropertiesLoader pros = new PropertiesLoader(args);
        ScriptGenerator script = new ScriptGenerator(pros.getProperties());
        script.generate();
    }
}