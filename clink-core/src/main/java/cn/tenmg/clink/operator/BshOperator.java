package cn.tenmg.clink.operator;

import java.util.List;
import java.util.Map;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import bsh.EvalError;
import bsh.Interpreter;
import cn.tenmg.clink.model.Bsh;
import cn.tenmg.clink.model.bsh.Var;

/**
 * BeanShell操作执行器
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.0
 */
public class BshOperator extends AbstractOperator<Bsh> {

	private static final String ENV = "env", DATA_HOLDER = "DataHolder";

	@Override
	public Object execute(StreamExecutionEnvironment env, Bsh bsh, Map<String, Object> params) throws Exception {
		Interpreter interpreter = new Interpreter();
		String java = bsh.getJava();
		List<Var> vars = bsh.getVars();
		interpreter.set(ENV, env);
		interpreter.set(DATA_HOLDER, params);
		setVars(interpreter, vars, params);
		return interpreter.eval(java);
	}

	private static void setVars(Interpreter interpreter, List<Var> vars, Map<String, Object> params) throws EvalError {
		if (vars == null || vars.isEmpty()) {
			return;
		}
		Var var;
		String name, value;
		Object param;
		for (int i = 0, size = vars.size(); i < size; i++) {
			var = vars.get(i);
			name = var.getName();
			value = var.getValue();
			if (name != null) {
				param = null;
				if (value == null) {
					param = params.get(name);
				} else {
					param = params.get(value);
					if (param == null) {
						param = value;
					}
				}
				interpreter.set(name, param);
			}
		}
	}

}
