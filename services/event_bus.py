"""
EventBus - Sistema de eventos para comunicación entre servicios
Permite desacoplar componentes y reaccionar a cambios en tiempo real
"""
import logging
import asyncio
from typing import Dict, List, Callable, Any
from datetime import datetime

logger = logging.getLogger(__name__)

class EventBus:
    """
    Sistema de eventos asíncrono para comunicación entre servicios
    Permite que servicios se suscriban a eventos y reaccionen en tiempo real
    """

    def __init__(self):
        self._listeners: Dict[str, List[Callable]] = {}
        self._debounce_timers: Dict[str, asyncio.Task] = {}

    def on(self, event_name: str, callback: Callable):
        """
        Registrar un listener para un evento específico

        Args:
            event_name: Nombre del evento (ej: 'candles_updated')
            callback: Función async a ejecutar cuando ocurra el evento
        """
        if event_name not in self._listeners:
            self._listeners[event_name] = []

        self._listeners[event_name].append(callback)
        logger.info(f"[EVENT BUS] Registered listener for '{event_name}'")

    async def emit(self, event_name: str, data: Any = None):
        """
        Emitir un evento a todos los listeners suscritos

        Args:
            event_name: Nombre del evento a emitir
            data: Datos del evento (opcional)
        """
        if event_name not in self._listeners:
            return

        logger.debug(f"[EVENT BUS] Emitting '{event_name}' to {len(self._listeners[event_name])} listeners")

        # Ejecutar todos los callbacks de forma asíncrona
        tasks = []
        for callback in self._listeners[event_name]:
            tasks.append(callback(data))

        # Esperar a que todos los callbacks terminen
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def emit_debounced(self, event_name: str, data: Any = None, delay: int = 5):
        """
        Emitir un evento con debounce (espera antes de ejecutar)
        Si se llama múltiples veces, solo ejecuta una vez después del último delay

        Args:
            event_name: Nombre del evento
            data: Datos del evento
            delay: Segundos de delay (default: 5)
        """
        debounce_key = f"{event_name}_debounced"

        # Cancelar timer anterior si existe
        if debounce_key in self._debounce_timers:
            self._debounce_timers[debounce_key].cancel()

        # Crear nuevo timer
        async def delayed_emit():
            await asyncio.sleep(delay)
            logger.info(f"[EVENT BUS] Debounced event '{event_name}' firing after {delay}s delay")
            await self.emit(event_name, data)
            # Limpiar timer
            if debounce_key in self._debounce_timers:
                del self._debounce_timers[debounce_key]

        self._debounce_timers[debounce_key] = asyncio.create_task(delayed_emit())
        logger.debug(f"[EVENT BUS] Debounced '{event_name}' scheduled ({delay}s delay)")

    def remove_listener(self, event_name: str, callback: Callable):
        """Remover un listener específico"""
        if event_name in self._listeners:
            self._listeners[event_name] = [
                cb for cb in self._listeners[event_name] if cb != callback
            ]

    def clear(self, event_name: str = None):
        """
        Limpiar listeners

        Args:
            event_name: Nombre específico a limpiar, o None para limpiar todos
        """
        if event_name:
            self._listeners[event_name] = []
        else:
            self._listeners.clear()

        logger.info(f"[EVENT BUS] Cleared listeners for '{event_name or 'all events'}'")

# Singleton instance
event_bus = EventBus()
