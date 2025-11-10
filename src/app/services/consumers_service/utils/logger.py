import logging
import os
# Nota: Se ha eliminado la importación de RotatingFileHandler

def setup_logger(name: str, log_file: str, level=logging.INFO):
    """Crea y configura un logger con salida a archivo y consola."""
    
    # Asegura que la carpeta de logs exista.
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    # Formato de logging (simplificado)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # 1. Handler para Archivo (sin rotación)
    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)

    # 2. Handler para Consola (StreamHandler)
    console = logging.StreamHandler()
    console.setFormatter(formatter)

    # 3. Configuración del Logger
    logger = logging.getLogger(name)
    
    # Esto previene la adición múltiple de handlers
    if logger.handlers:
        for existing_handler in logger.handlers:
            logger.removeHandler(existing_handler)
            
    logger.setLevel(level)
    logger.addHandler(handler)
    logger.addHandler(console)

    return logger