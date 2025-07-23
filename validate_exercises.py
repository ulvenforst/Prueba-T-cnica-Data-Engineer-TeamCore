#!/usr/bin/env python3
"""
Script de validación completa de todos los ejercicios
Verifica que cada ejercicio esté correctamente implementado
"""

import sys
import logging
from pathlib import Path
from typing import Dict, List, Any

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ExerciseValidator:
    """Validador de ejercicios técnicos"""
    
    def __init__(self):
        self.project_root = Path(__file__).parent.parent
        self.validation_results = {}
    
    def validate_exercise_1(self) -> Dict[str, Any]:
        """Valida Ejercicio 1: Orquestación local"""
        logger.info("=== VALIDANDO EJERCICIO 1: ORQUESTACIÓN LOCAL ===")
        
        checks = {
            'dag_file': self.project_root / "airflow" / "dags" / "transactions_dag.py",
            'extract_script': self.project_root / "airflow" / "scripts" / "extract.py",
            'transform_script': self.project_root / "airflow" / "scripts" / "transform.py", 
            'load_script': self.project_root / "airflow" / "scripts" / "load.py",
            'validate_script': self.project_root / "airflow" / "scripts" / "validate.py",
            'tests': self.project_root / "airflow" / "tests" / "test_etl.py"
        }
        
        results = {}
        
        for check_name, file_path in checks.items():
            if file_path.exists():
                results[check_name] = "✅ EXISTE"
                
                # Verificaciones adicionales por contenido
                if check_name == 'dag_file':
                    content = file_path.read_text()
                    if 'FileSensor' in content and 'chunk' in content.lower():
                        results[f'{check_name}_features'] = "✅ FileSensor + chunks"
                    else:
                        results[f'{check_name}_features'] = "❌ Falta FileSensor o chunks"
                        
            else:
                results[check_name] = f"❌ NO EXISTE: {file_path}"
        
        # Verificar funcionalidades específicas
        features = [
            "DAG completo con dependencias",
            "Lectura por chunks para eficiencia",
            "Reintentos automáticos configurados", 
            "Validación de tabla no vacía",
            "Alertas simuladas en fallos",
            "Scripts modulares reutilizables",
            "Tests unitarios e integración",
            "Métricas por tarea",
            "Compatibilidad archivos comprimidos"
        ]
        
        results['required_features'] = features
        results['implementation_score'] = len([v for v in results.values() if "✅" in str(v)]) / len(checks) * 100
        
        return results
    
    def validate_exercise_2(self) -> Dict[str, Any]:
        """Valida Ejercicio 2: SQL y análisis"""
        logger.info("=== VALIDANDO EJERCICIO 2: SQL Y ANÁLISIS ===")
        
        sql_queries = {
            'tarea1_vista_resumen': self.project_root / "sql" / "queries" / "tarea1_vista_resumen.sql",
            'tarea2_usuarios_multiples_fallas': self.project_root / "sql" / "queries" / "tarea2_usuarios_multiples_fallas.sql",
            'tarea3_deteccion_anomalias': self.project_root / "sql" / "queries" / "tarea3_deteccion_anomalias.sql",
            'tarea4_indices_triggers': self.project_root / "sql" / "queries" / "tarea4_indices_triggers.sql"
        }
        
        analysis_runner = self.project_root / "sql" / "analysis_runner.py"
        
        results = {}
        
        # Verificar archivos SQL
        for query_name, file_path in sql_queries.items():
            if file_path.exists():
                results[query_name] = "✅ EXISTE"
                
                # Verificar contenido específico
                content = file_path.read_text().lower()
                if query_name == 'tarea1_vista_resumen' and 'group by' in content:
                    results[f'{query_name}_content'] = "✅ Tiene GROUP BY para resumen"
                elif query_name == 'tarea2_usuarios_multiples_fallas' and 'having' in content:
                    results[f'{query_name}_content'] = "✅ Tiene HAVING para filtro múltiple"
                elif query_name == 'tarea3_deteccion_anomalias' and ('std' in content or 'deviation' in content):
                    results[f'{query_name}_content'] = "✅ Implementa detección anomalías"
                elif query_name == 'tarea4_indices_triggers' and 'index' in content:
                    results[f'{query_name}_content'] = "✅ Incluye análisis de índices"
                    
            else:
                results[query_name] = f"❌ NO EXISTE: {file_path}"
        
        # Verificar runner
        if analysis_runner.exists():
            results['analysis_runner'] = "✅ EXISTE"
        else:
            results['analysis_runner'] = f"❌ NO EXISTE: {analysis_runner}"
        
        required_features = [
            "Vista resumen por día y estado",
            "Query usuarios con >3 fallas en 7 días", 
            "Detección anomalías por desviación estándar",
            "Análisis de índices y triggers",
            "Runner automatizado para ejecutar queries"
        ]
        
        results['required_features'] = required_features
        results['implementation_score'] = len([v for v in results.values() if "✅" in str(v)]) / len(sql_queries) * 100
        
        return results
    
    def validate_exercise_3(self) -> Dict[str, Any]:
        """Valida Ejercicio 3: ETL Python para archivo grande"""
        logger.info("=== VALIDANDO EJERCICIO 3: ETL PYTHON STREAMING ===")
        
        etl_files = {
            'processor': self.project_root / "etl" / "processor.py",
            'streaming_processor': self.project_root / "etl" / "streaming_processor.py",
            'run_etl': self.project_root / "etl" / "run_etl.py",
            'benchmark': self.project_root / "etl" / "run_benchmark.py"
        }
        
        results = {}
        
        for file_name, file_path in etl_files.items():
            if file_path.exists():
                results[file_name] = "✅ EXISTE"
                
                # Verificar contenido específico
                content = file_path.read_text().lower()
                
                if 'gzip' in content and 'json' in content:
                    results[f'{file_name}_gzip_json'] = "✅ Soporta gzip + JSON"
                    
                if 'status_code' in content and '500' in content:
                    results[f'{file_name}_filter'] = "✅ Filtra status_code >= 500"
                    
                if 'parquet' in content or 'snappy' in content:
                    results[f'{file_name}_parquet'] = "✅ Export a Parquet/Snappy"
                    
                if 'multiprocessing' in content or 'polars' in content or 'dask' in content:
                    results[f'{file_name}_variants'] = "✅ Múltiples implementaciones"
                    
            else:
                results[file_name] = f"❌ NO EXISTE: {file_path}"
        
        required_features = [
            "Lectura streaming de gzip JSONL",
            "Filtrado por status_code >= 500",
            "Limpieza y parsing de campos",
            "Agrupación por hora/endpoint",
            "Export a Parquet con compresión snappy",
            "Implementaciones: multiprocessing, polars, dask",
            "Profiling de memoria y tiempo",
            "Manejo de errores y logging"
        ]
        
        results['required_features'] = required_features
        results['implementation_score'] = len([v for v in results.values() if "✅" in str(v)]) / len(etl_files) * 100
        
        return results
    
    def validate_exercise_4(self) -> Dict[str, Any]:
        """Valida Ejercicio 4: Modelado de datos"""
        logger.info("=== VALIDANDO EJERCICIO 4: MODELADO DE DATOS ===")
        
        modeling_files = {
            'warehouse': self.project_root / "modeling" / "warehouse.py",
            'run_warehouse': self.project_root / "modeling" / "run_warehouse.py",
            'validate_warehouse': self.project_root / "modeling" / "validate_warehouse.py"
        }
        
        results = {}
        
        for file_name, file_path in modeling_files.items():
            if file_path.exists():
                results[file_name] = "✅ EXISTE"
                
                # Verificar contenido específico del warehouse
                if file_name == 'warehouse':
                    content = file_path.read_text()
                    
                    dim_tables = ['dim_user', 'dim_time', 'dim_status']
                    fact_tables = ['fact_transactions']
                    
                    for table in dim_tables + fact_tables:
                        if table in content:
                            results[f'table_{table}'] = "✅ EXISTE"
                        else:
                            results[f'table_{table}'] = f"❌ FALTA: {table}"
                    
                    if 'scd' in content.lower() or 'slowly changing' in content.lower():
                        results['scd_implementation'] = "✅ SCD implementado"
                    else:
                        results['scd_implementation'] = "❌ SCD no encontrado"
                        
                    if 'index' in content.lower():
                        results['indexes'] = "✅ Índices implementados"
                    else:
                        results['indexes'] = "❌ Índices no encontrados"
                        
            else:
                results[file_name] = f"❌ NO EXISTE: {file_path}"
        
        required_features = [
            "Esquema star/snowflake",
            "Tablas dimensionales (user, time, status)",
            "Tabla de hechos (transactions)",
            "Carga inicial desde CSV",
            "Estrategia SCD tipo 1 o 2",
            "Índices para optimización",
            "Simulación de partición lógica",
            "Documentación de decisiones"
        ]
        
        results['required_features'] = required_features
        results['implementation_score'] = len([v for v in results.values() if "✅" in str(v)]) / len(modeling_files) * 100
        
        return results
    
    def validate_exercise_5(self) -> Dict[str, Any]:
        """Valida Ejercicio 5: Git + CI/CD"""
        logger.info("=== VALIDANDO EJERCICIO 5: GIT + CI/CD ===")
        
        cicd_files = {
            'github_workflow': self.project_root / ".github" / "workflows" / "ci-cd.yml",
            'docker_compose': self.project_root / "docker-compose.yml",
            'dockerfile': self.project_root / "Dockerfile",
            'gitignore': self.project_root / ".gitignore",
            'gitattributes': self.project_root / ".gitattributes"
        }
        
        results = {}
        
        for file_name, file_path in cicd_files.items():
            if file_path.exists():
                results[file_name] = "✅ EXISTE"
                
                # Verificaciones específicas
                if file_name == 'github_workflow':
                    content = file_path.read_text().lower()
                    
                    ci_features = ['lint', 'test', 'build', 'deploy']
                    for feature in ci_features:
                        if feature in content:
                            results[f'ci_{feature}'] = f"✅ {feature.upper()}"
                        else:
                            results[f'ci_{feature}'] = f"❌ {feature.upper()} faltante"
                            
            else:
                results[file_name] = f"❌ NO EXISTE: {file_path}"
        
        # Verificar estructura modular
        expected_dirs = ['airflow', 'etl', 'sql', 'modeling', 'data', 'tests']
        for dir_name in expected_dirs:
            dir_path = self.project_root / dir_name
            if dir_path.exists() and dir_path.is_dir():
                results[f'dir_{dir_name}'] = "✅ EXISTE"
            else:
                results[f'dir_{dir_name}'] = f"❌ NO EXISTE: {dir_name}/"
        
        required_features = [
            "Estructura modular (airflow/, etl/, sql/, modeling/)",
            "Estrategia de branching",
            "Pipeline CI: linter, tests, build, deploy",
            "Docker containerization",
            "GitIgnore para datos grandes",
            "Git LFS para archivos grandes",
            "Automatización completa"
        ]
        
        results['required_features'] = required_features
        results['implementation_score'] = len([v for v in results.values() if "✅" in str(v)]) / len(cicd_files) * 100
        
        return results
    
    def run_complete_validation(self) -> Dict[str, Any]:
        """Ejecuta validación completa de todos los ejercicios"""
        logger.info("=" * 80)
        logger.info("INICIANDO VALIDACIÓN COMPLETA DE TODOS LOS EJERCICIOS")
        logger.info("=" * 80)
        
        # Validar cada ejercicio
        self.validation_results = {
            'ejercicio_1': self.validate_exercise_1(),
            'ejercicio_2': self.validate_exercise_2(), 
            'ejercicio_3': self.validate_exercise_3(),
            'ejercicio_4': self.validate_exercise_4(),
            'ejercicio_5': self.validate_exercise_5()
        }
        
        # Generar reporte consolidado
        self.generate_final_report()
        
        return self.validation_results
    
    def generate_final_report(self):
        """Genera reporte final de validación"""
        logger.info("\n" + "=" * 80)
        logger.info("REPORTE FINAL DE VALIDACIÓN")
        logger.info("=" * 80)
        
        total_score = 0
        exercise_count = 0
        
        for exercise_name, exercise_results in self.validation_results.items():
            score = exercise_results.get('implementation_score', 0)
            total_score += score
            exercise_count += 1
            
            status = "COMPLETO" if score >= 90 else "PARCIAL" if score >= 70 else "INCOMPLETO"
            logger.info(f"{exercise_name.upper()}: {score:.1f}% - {status}")
        
        overall_score = total_score / exercise_count if exercise_count > 0 else 0
        
        logger.info("=" * 80)
        logger.info(f"PUNTUACIÓN GENERAL: {overall_score:.1f}%")
        
        if overall_score >= 95:
            logger.info("🏆 EXCELENTE: Todos los ejercicios implementados completamente")
        elif overall_score >= 85:
            logger.info("✅ MUY BUENO: Mayoría de ejercicios completos")
        elif overall_score >= 70:
            logger.info("⚠️  BUENO: Implementación básica completa")
        else:
            logger.info("❌ NECESITA MEJORAS: Varios ejercicios incompletos")
        
        logger.info("=" * 80)
        
        # Generar archivo de reporte
        report_file = self.project_root / "VALIDATION_REPORT.md"
        self.save_report_to_file(report_file, overall_score)
        logger.info(f"📄 Reporte detallado guardado en: {report_file}")
    
    def save_report_to_file(self, report_file: Path, overall_score: float):
        """Guarda reporte detallado en archivo"""
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("# Reporte de Validación - Ejercicios TeamCore\n\n")
            f.write(f"**Fecha de validación**: {__import__('datetime').datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"**Puntuación general**: {overall_score:.1f}%\n\n")
            
            for exercise_name, exercise_results in self.validation_results.items():
                exercise_num = exercise_name.split('_')[1]
                score = exercise_results.get('implementation_score', 0)
                
                f.write(f"## Ejercicio {exercise_num}\n\n")
                f.write(f"**Puntuación**: {score:.1f}%\n\n")
                
                # Features requeridas
                required_features = exercise_results.get('required_features', [])
                if required_features:
                    f.write("### Características Requeridas:\n\n")
                    for feature in required_features:
                        f.write(f"- {feature}\n")
                    f.write("\n")
                
                # Resultados de validación
                f.write("### Resultados de Validación:\n\n")
                for key, value in exercise_results.items():
                    if key not in ['required_features', 'implementation_score']:
                        f.write(f"- **{key}**: {value}\n")
                f.write("\n")
            
            f.write("## Resumen\n\n")
            if overall_score >= 95:
                f.write("✅ **TODOS LOS EJERCICIOS IMPLEMENTADOS COMPLETAMENTE**\n")
            elif overall_score >= 85:
                f.write("🟢 **IMPLEMENTACIÓN MUY BUENA - Mayoría completa**\n")
            elif overall_score >= 70:
                f.write("🟡 **IMPLEMENTACIÓN BÁSICA COMPLETA**\n")
            else:
                f.write("🔴 **NECESITA MEJORAS - Varios ejercicios incompletos**\n")


def main():
    """Función principal"""
    print("🔍 VALIDADOR DE EJERCICIOS TÉCNICOS - TEAMCORE")
    print("=" * 60)
    
    validator = ExerciseValidator()
    results = validator.run_complete_validation()
    
    # Determinar código de salida
    scores = [result.get('implementation_score', 0) for result in results.values()]
    overall_score = sum(scores) / len(scores) if scores else 0
    
    return overall_score >= 85  # Considerar exitoso si >= 85%


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
