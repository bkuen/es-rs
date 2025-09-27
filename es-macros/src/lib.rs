// event_registry_derive/src/lib.rs

use proc_macro::TokenStream;
use quote::{quote, format_ident};
use syn::{parse_macro_input, DeriveInput, Data, Fields, Lit, MetaNameValue, Token, Expr, ExprLit, punctuated::Punctuated, Path, DataStruct, ItemStruct, Meta};

#[proc_macro_attribute]
pub fn view_store(attrs: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemStruct);
    let attrs = parse_macro_input!(attrs with Punctuated::<Meta, syn::Token![,]>::parse_terminated);
    let ident = &input.ident;
    let vis = &input.vis;

    let mut view_idents = Vec::new();

    for attr in &attrs {
        if attr.path().is_ident("views") {
            if let Meta::List(meta_list) = attr {
                if let Ok(args) = meta_list.parse_args_with(Punctuated::<Path, Token![,]>::parse_terminated) {
                    for path in args {
                        if let Some(ident) = path.get_ident() {
                            view_idents.push(ident.clone());
                        }
                    }
                } else {
                    return syn::Error::new_spanned(attr, "Expected a list of view identifiers").to_compile_error().into();
                }
            } else {
                return syn::Error::new_spanned(attr, "Expected #[views(...)] attribute with a list of view identifiers").to_compile_error().into();
            }

        } else {
            return syn::Error::new_spanned(attr, "Expected #[views(...)] attribute").to_compile_error().into();
        }
    }

    let mut wrapper_variants = Vec::new();
    let mut view_from_impls = Vec::new();

    for view_ident in view_idents.iter() {
        wrapper_variants.push(quote! {
            #view_ident(#view_ident)
        });

        view_from_impls.push(quote! {
            impl From<#view_ident> for ViewWrapper2 {
                 fn from(view: #view_ident) -> Self {
                    Self::#view_ident(view)
                 }
            }
        });
    }

    let expanded = quote! {
        pub enum ViewWrapper2 {
            #(#wrapper_variants),*
        }

         #(#view_from_impls)*

        #vis struct #ident {
            views: Arc<Mutex<Vec<ViewWrapper2>>>,
        }

        impl #ident {
            pub fn new() -> Self {
                Self {
                    views: Default::default(),
                }
            }

            pub async fn register_view<V, A>(&self, view: V)
            where
                V: View<A> + Into<ViewWrapper2> + Send + Sync + 'static,
                A: Aggregate + EventApplier + Send + Sync + 'static,
            {
                let mut views = self.views.lock().await;
                views.push(view.into());
            }
        }

        #[async_trait]
        impl ViewUpdater for #ident
        {
            async fn update_views<E>(&self, aggregate_id: Uuid, events: &[AggregateEvent<E>])
            where
                E: DomainEvent + Send + Sync + 'static,
            {
                let mut views = self.views.lock().await;
                for view in views.iter_mut() {
                    view.handle(aggregate_id, events).await;
                }
            }
        }
    };

    // panic!("{}", expanded);

    expanded.into()
}

#[proc_macro_derive(ViewRegistry, attributes(registry))]
pub fn derive_view_registry(input: TokenStream) -> TokenStream {
    // 1) Parse the enum
    let input = parse_macro_input!(input as DeriveInput);
    let enum_ident = &input.ident;
    let vis        = &input.vis;

    // 2) Read #[registry(name = "...")]
    let mut store_name = format_ident!("ViewRegistry");
    for attr in &input.attrs {
        if !attr.meta.path().is_ident("registry") { continue; }
        // Use parse_args_with to parse the attribute arguments
        if let Ok(args) = attr.parse_args_with(Punctuated::<MetaNameValue, Token![,]>::parse_terminated) {
            for nv in args {
                if nv.path.is_ident("name") {
                    if let Expr::Lit(ExprLit { lit: Lit::Str(s), .. }) = nv.value {
                        store_name = format_ident!("{}", s.value());
                    }
                }
            }
        }
    }

    // 3) Only enums are supported
    let variants = match &input.data {
        Data::Enum(e) => &e.variants,
        _ => {
            return syn::Error::new_spanned(
                &input.ident,
                "ViewRegistry can only be derived on enums"
            )
                .to_compile_error()
                .into();
        }
    };

    // 4) Collect info for each variant
    let mut field_defs    = Vec::new();
    let mut ctor_inits    = Vec::new();
    let mut match_arms    = Vec::new();
    let mut has_view_impl = Vec::new();

    for var in variants {
        let v_ident    = &var.ident;
        let field_name = format_ident!("{}_views", v_ident.to_string().to_lowercase());

        // expect exactly one unnamed field: AggregateEvent<SomeEvent>
        let ty = match &var.fields {
            Fields::Unnamed(u) if u.unnamed.len() == 1 => &u.unnamed[0].ty,
            _ => {
                return syn::Error::new_spanned(
                    &var.ident,
                    "Each variant must have exactly one unnamed field"
                )
                    .to_compile_error()
                    .into()
            }
        };

        // a) registry field
        field_defs.push(quote! {
            #field_name: Vec<Box<dyn View<#ty, Error = Error> + Send + Sync>>
        });

        // b) new() init
        ctor_inits.push(quote! {
            #field_name: Vec::new()
        });

        // c) match arm for update_from_events
        match_arms.push(quote! {
            #enum_ident::#v_ident(ev) => {
                let batch = std::slice::from_ref(ev);
                for v in &mut registry.#field_name {
                    v.update(ev.aggregate_id, batch).await?;
                }
            }
        });

        // d) HasViewField impl
        has_view_impl.push(quote! {
            impl HasViewField<#ty> for #store_name {
                fn views(&mut self)
                    -> &mut Vec<Box<dyn View<#ty, Error = Error> + Send + Sync>>
                {
                    &mut self.#field_name
                }
            }
        });
    }

    // 5) emit everything
    let expanded = quote! {
        #vis struct #store_name {
            #(#field_defs),*
        }

        impl #store_name {
            pub fn new() -> Self {
                Self { #(#ctor_inits),* }
            }

            /// single, type‑safe register for any `View<E>`
            pub fn register<E, V>(&mut self, view: V)
            where
                E: Clone + Send + Sync + 'static,
                V: View<E, Error = Error> + Send + Sync + 'static,
                Self: HasViewField<E>,
            {
                <Self as HasViewField<E>>::views(self).push(Box::new(view));
            }

            /// dispatch each variant to its bucket
            pub async fn update_from_events(
                &mut self,
                events: &[#enum_ident],
            ) -> Result<(), Error> {
                for ev in events {
                    match ev {
                        #(#match_arms),*,
                        _ => {},
                    }
                }
                Ok(())
            }
        }

        /// helper trait for compile‑time routing of `register`
        pub trait HasViewField<E> {
            fn views(&mut self)
                -> &mut Vec<Box<dyn View<E, Error = Error> + Send + Sync>>;
        }

        #(#has_view_impl)*

        // Implement ViewRegistry trait from es crate
        impl store::view::ViewRegistry for #enum_ident {
            type Registry = #store_name;

            fn register_view<V, A>(&mut self, view: V)
            where
                V: store::view::View<Self> + Send + Sync + 'static,
                A: ddd::aggregate::Aggregate + EventApplier + 'static;
            {
                Self::register(self, view);
            }

            async fn dispatch_all(
                registry: &mut Self::Registry,
                events: &[Self],
            ) -> Result<(), std::boxed::Box<dyn std::error::Error + Send + Sync>> {
                registry.update_from_events(events).await.map_err(|e| std::boxed::Box::new(e) as std::boxed::Box<dyn std::error::Error + Send + Sync>)
            }
        }
    };

    expanded.into()
}
