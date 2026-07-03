# Cómo se lo explicaría al junior

Arrancaría por lo que resolvió bien: el cálculo de negocio (la conversión CS a ST
y el revenue) está correcto, y el filtro de rutina también. Eso importa. El
feedback empieza por lo que funciona, no por una lista de errores.

Después llevaría la charla a una sola idea de fondo, no a los cinco puntos de
golpe: "estás usando Spark como si fuera pandas". De ahí salen casi todos los
problemas (el bucle fila por fila, todo en memoria del driver, la escritura no
idempotente). Le pondría al lado su `iterrows` y la versión con `when`/`otherwise`
y correríamos las dos, para que vea la diferencia de rendimiento y de intención.
Un aprendizaje que se ve pega más que una corrección que se dicta.

El segundo tema sería hardcoding contra configuración, atado al requisito
multi-tenant: si mañana entra otro país, ¿qué tenés que tocar? Esa pregunta lo
hace darse cuenta solo de por qué el país y las rutas tienen que ser parámetros.

Lo de estilo (naming, `print` contra `logging`, la sesión global) lo dejaría
escrito en el PR, sin gastar la charla en vivo en eso. Son correcciones mecánicas
que no necesitan discusión.

Le pediría investigar tres cosas puntuales, con nombres para buscar:

1. Idempotencia en pipelines: `MERGE INTO` y overwrite por partición en Delta.
2. Por qué Delta sobre Parquet: transacciones ACID y time travel.
3. El patrón de cuarentena: por qué no se descartan anomalías en silencio en una
   plataforma gobernada.

Cerraría acordando que refactorice él un módulo chico con estas ideas y lo
revisemos juntos. La idea no es que entregue mi solución, sino que la próxima vez
llegue solo.
